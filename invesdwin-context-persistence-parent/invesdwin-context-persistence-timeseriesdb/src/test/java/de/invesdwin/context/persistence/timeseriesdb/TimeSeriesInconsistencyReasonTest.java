package de.invesdwin.context.persistence.timeseriesdb;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache.InconsistencyReason;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.AMemoryFileSummarySerializingCollection;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.MemoryFileMetadata;
import de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdateProgress;
import de.invesdwin.context.system.properties.ICloseableProperties;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.string.Charsets;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;

/**
 * The answer a recovery decision needs is not "inconsistent?" but "inconsistent HOW?".
 * <p>
 * Every consumer of a timeseriesdb store treats one boolean as a licence to delete the key and re-fetch it. That is
 * the right act for some states and a data-destroying act for others: a store whose files are all present but whose
 * byte attestation drifted is not the same case as a store whose payload lost bytes, yet both answer true, and today
 * both get wiped. This class pins the reason of each state as its own protected surface, so a later recovery can be
 * written per reason instead of all-or-nothing. It changes no behaviour: the boolean still answers exactly what it
 * answered before, and every cell asserts the boolean AND the reason together so the two cannot drift apart.
 * <p>
 * One reason is deliberately ambiguous and is pinned AS ambiguous rather than coded around:
 * {@link InconsistencyReason#HIGH_WATER_AHEAD_OF_CHAIN} covers both "the number is stale" and "the chain lost its
 * tail whole" (a payload file and the summaries naming it gone together). In the second case the shortened chain is
 * self-consistent by construction, and the attestation is the only witness that anything was ever there - no reader
 * can tell the two apart, which is why the reset stays the answer and why a non-wiping recovery has to live where an
 * attempt is about to publish, not in this check.
 */
@NotThreadSafe
public class TimeSeriesInconsistencyReasonTest extends ATest {

    private static final int BATCH_FLUSH_INTERVAL = 4;
    private static final String KEY = "reasonKey";

    private File baseDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY, TimeSeriesInconsistencyReasonTest.class.getName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Files.deleteQuietly(baseDirectory);
    }

    @Test
    public void testAHonestStoreRecordsNoReason() throws Exception {
        final TimeSeriesLookupStorageCache<String, FDate> lookup = openStore("clean");
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(), "a fresh pass leaves a consistent store");
        Assertions.checkEquals(InconsistencyReason.NONE, lookup.getLastInconsistencyReason(),
                "and nothing was recorded against it");
    }

    /**
     * The attestation names more bytes than the chain accounts for and carries no generation key - the state a store
     * populated before the pairing existed lives in. The boolean must stay true (this is the other side of
     * TimeSeriesAttestedGenerationCrashWindowTest's pin), and the reason must name WHICH arm indicted it.
     */
    @Test
    public void testAnUnpairedAttestationAheadOfTheChainReasonsHighWaterAhead() throws Exception {
        final TimeSeriesLookupStorageCache<String, FDate> lookup = openStore("unpairedAhead");
        final long highWater = readAttestation(lookup);
        //java.nio on purpose: this is what a pre-pairing build's properties file looks like - one key, no generation
        final File propertiesFile = new File(new File(memoryDirectoryOf(lookup), "properties"), "memory.properties");
        Assertions.checkTrue(propertiesFile.exists(), "the publish writes its attestation there: " + propertiesFile);
        java.nio.file.Files.write(propertiesFile.toPath(),
                ("EXPECTED_MEMORY_FILE_SIZE=" + (highWater + 1000L) + "\n").getBytes(Charsets.UTF_8));
        lookup.clearCaches();

        Assertions.checkTrue(lookup.isEmptyOrInconsistent(), "the reset answer must not change here");
        Assertions.checkEquals(InconsistencyReason.HIGH_WATER_AHEAD_OF_CHAIN, lookup.getLastInconsistencyReason(),
                "but the reason must say it was the high-water comparison, not a record check");
    }

    /**
     * The paired crash window: an attestation naming a generation that never landed is not an indictment at all, so
     * the recorded reason stays NONE even though the two numbers disagree.
     */
    @Test
    public void testACrashWindowAttestationRecordsNoReason() throws Exception {
        final TimeSeriesLookupStorageCache<String, FDate> lookup = openStore("crashWindow");
        final MemoryFileMetadata metadata = lookup.getMemoryFileLookupTable().getMetadata();
        final long highWater = readAttestation(lookup);
        final int generation = lookup.getMemoryFileLookupTable().getCurrentIndexNumber();
        try (ICloseableProperties properties = metadata.getProperties()) {
            metadata.setExpectedMemoryFileSize(properties, highWater + 4096L, generation + 1);
        }
        lookup.clearCaches();

        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "an attestation of a generation that never landed leaves the chain alone");
        Assertions.checkEquals(InconsistencyReason.NONE, lookup.getLastInconsistencyReason(),
                "so no reason is recorded - the skip and the clean case must not be told apart by a reason either");
    }

    /**
     * A payload that a published summary names, with its bytes removed: the chain's own arithmetic exceeds what is on
     * disk. Distinct from the attestation arm - here the data really is gone.
     */
    @Test
    public void testATruncatedNamedPayloadReasonsFileShorterThanClaimed() throws Exception {
        final TimeSeriesLookupStorageCache<String, FDate> lookup = openStore("truncated");
        final File named = new File(publishedSummaries(lookup).get(0).getMemoryResourceUri());
        try (RandomAccessFile raf = new RandomAccessFile(named, "rw")) {
            raf.setLength(0L);
        }
        lookup.clearCaches();

        Assertions.checkTrue(lookup.isEmptyOrInconsistent(), "a chain naming an empty file is damaged");
        Assertions.checkEquals(InconsistencyReason.NAMED_FILE_SHORTER_THAN_CLAIMED, lookup.getLastInconsistencyReason(),
                "and the reason is the per-record length check, not the high-water comparison");
    }

    /**
     * The inverse byte state: a published file holding MORE bytes than any account of it - unpublished trailing bytes
     * in a published file, which the publish rules of TimeSeriesUpdateTransaction#commitSegmentBoundary forbid.
     */
    @Test
    public void testAPublishedFileWithTrailingBytesReasonsFilesBeyondHighWater() throws Exception {
        final TimeSeriesLookupStorageCache<String, FDate> lookup = openStore("trailing");
        final List<MemoryFileSummary> published = publishedSummaries(lookup);
        final File named = new File(published.get(published.size() - 1).getMemoryResourceUri());
        try (RandomAccessFile raf = new RandomAccessFile(named, "rw")) {
            raf.setLength(named.length() + 5L);
        }
        lookup.clearCaches();

        Assertions.checkTrue(lookup.isEmptyOrInconsistent(), "the store's own length check still indicts it");
        Assertions.checkEquals(InconsistencyReason.FILES_BEYOND_HIGH_WATER, lookup.getLastInconsistencyReason(),
                "and the reason says the files, not the number, are the outlier");
    }

    @Test
    public void testAMissingUpdatedMarkerReasonsNoUpdatedMarker() throws Exception {
        final TimeSeriesLookupStorageCache<String, FDate> lookup = openStore("noMarker");
        final File marker = lookup.getDirectoryHashKey().getDirectoryHashKeyVersion().getUpdatedMarkerFile();
        Assertions.checkTrue(marker.exists(), "the pass wrote the marker: " + marker);
        Assertions.checkTrue(marker.delete(), "removing it must work: " + marker);
        lookup.clearCaches();

        Assertions.checkTrue(lookup.isEmptyOrInconsistent(), "a store that never completed a publish is empty");
        Assertions.checkEquals(InconsistencyReason.NO_UPDATED_MARKER, lookup.getLastInconsistencyReason(),
                "which is a different case from a damaged chain, and has nothing to recover");
    }

    @Test
    public void testAStoreWithNoGenerationFilesReasonsNoSummaries() throws Exception {
        final TimeSeriesLookupStorageCache<String, FDate> lookup = openStore("noGenerations");
        final File memoryDirectory = memoryDirectoryOf(lookup);
        final List<File> generations = new ArrayList<>();
        for (final File candidate : memoryDirectory.listFiles()) {
            if (candidate.getName().endsWith("_" + AMemoryFileSummarySerializingCollection.MEMORY_INDEX_FILE_NAME)) {
                generations.add(candidate);
            }
        }
        Assertions.checkTrue(!generations.isEmpty(),
                "precondition: the pass published generation files into " + memoryDirectory);
        for (final File generation : generations) {
            Assertions.checkTrue(generation.delete(), "removing it must work: " + generation);
        }
        lookup.clearCaches();

        Assertions.checkTrue(lookup.isEmptyOrInconsistent(), "a chain of nothing is not a store to serve");
        Assertions.checkEquals(InconsistencyReason.NO_SUMMARIES, lookup.getLastInconsistencyReason(),
                "and the reason separates it from a chain that lost bytes");
    }

    // --- plumbing ---

    private static long readAttestation(final TimeSeriesLookupStorageCache<String, FDate> lookup) {
        final MemoryFileMetadata metadata = lookup.getMemoryFileLookupTable().getMetadata();
        try (ICloseableProperties properties = metadata.getProperties()) {
            return metadata.getExpectedMemoryFileSize(properties);
        }
    }

    private static File memoryDirectoryOf(final TimeSeriesLookupStorageCache<String, FDate> lookup) {
        return lookup.getDirectoryHashKeyVersionMemory().getDirectoryHashKeyVersionDataShared();
    }

    private static List<MemoryFileSummary> publishedSummaries(
            final TimeSeriesLookupStorageCache<String, FDate> lookup) {
        final List<MemoryFileSummary> summaries = new ArrayList<>();
        try (ICloseableIterator<MemoryFileSummary> it = lookup.getMemoryFileLookupTable().range()) {
            Lists.toListWithoutHasNext(it, summaries);
        }
        return summaries;
    }

    private TimeSeriesLookupStorageCache<String, FDate> openStore(final String name)
            throws IncompleteUpdateRetryableException {
        final ATimeSeriesDB<String, FDate> table = newTable(name);
        newUpdater(table, newValues(1, BATCH_FLUSH_INTERVAL + 2)).update().close();
        return table.getLookupTableCache(KEY);
    }

    private static List<FDate> newValues(final int fromDay, final int toDay) {
        final List<FDate> values = new ArrayList<>();
        for (int day = fromDay; day <= toDay; day++) {
            values.add(FDateBuilder.newDate(2026, 1, day));
        }
        return values;
    }

    private ATimeSeriesDB<String, FDate> newTable(final String name) {
        return new ATimeSeriesDB<String, FDate>(name) {

            @Override
            protected ISerde<FDate> newValueSerde() {
                return FDateSerde.GET;
            }

            @Override
            protected Integer newValueFixedLength() {
                return FDateSerde.FIXED_LENGTH;
            }

            @Override
            protected int newBatchFlushInterval() {
                return BATCH_FLUSH_INTERVAL;
            }

            @Override
            public ITimeSeriesBaseDirectory getBaseDirectory() {
                return new TimeSeriesBaseDirectory(baseDirectory);
            }

            @Override
            public String innerHashKeyToString(final String key) {
                return key;
            }

            @Override
            public FDate extractStartTime(final FDate value) {
                return value;
            }

            @Override
            public FDate extractEndTime(final FDate value) {
                return value;
            }
        };
    }

    private ATimeSeriesUpdater<String, FDate> newUpdater(final ATimeSeriesDB<String, FDate> table,
            final List<FDate> values) {
        return new ATimeSeriesUpdater<String, FDate>(KEY, table) {

            @Override
            protected ICloseableIterable<? extends FDate> getSource(final FDate updateFrom) {
                return WrapperCloseableIterable.maybeWrap(values);
            }

            @Override
            protected void onUpdateStarted(final FDate updateStart) {}

            @Override
            protected void onUpdateFinished() {}

            @Override
            protected FDate extractStartTime(final FDate element) {
                return element;
            }

            @Override
            protected FDate extractEndTime(final FDate element) {
                return element;
            }

            @Override
            protected void onElement(final ITimeSeriesUpdateProgress relativeProgress, final long relativeCount) {}

            @Override
            protected void onFlush(final ITimeSeriesUpdateProgress relativeProgress, final long flushIndex) {}

            @Override
            public Percent getProgress(final FDate minTime, final FDate maxTime) {
                return null;
            }
        };
    }
}
