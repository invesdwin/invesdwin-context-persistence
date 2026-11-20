package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache.InconsistencyReason;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.AMemoryFileSummarySerializingCollection;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.MemoryFileMetadata;
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
import de.invesdwin.util.time.date.FDates;

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

    /**
     * The failure case the fleet actually hits: an update pass that dies part way through - a surefire fork timeout,
     * a kill -9, an OOM termination, a hung process someone stops - after an earlier pass had already published. What
     * the killed pass must NOT do is leave the store in a state the next reader calls damaged, because the next reader
     * answers that by wiping the key. This is the 2026-11-20 parallel-test signature seen from the store side: run 1
     * was cap-killed mid-publish, run 2 inherited its leftovers and could not make progress.
     * <p>
     * A pass that dies before crossing the publish cadence attests nothing (its unfinished chunks are never
     * summarised), so the surviving attestation is the healthy one from the completed pass, still paired with its
     * generation.
     */
    @Test
    public void testAPassDyingAfterAnEarlierPublishLeavesTheNextReaderACleanStore() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("killedAfterPublish");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final List<FDate> landed = newValues(1, BATCH_FLUSH_INTERVAL + 2);
        newUpdater(table, landed).update().close();

        //a second pass that dies mid-way: enough values to look like a long pass, a null to stop it
        final List<FDate> dying = new ArrayList<>(newValues(1, TimeSeriesUpdateTransaction.PUBLISH_SEGMENTS_PER_GENERATION * BATCH_FLUSH_INTERVAL - 1));
        dying.add(null);
        try {
            newUpdater(table, dying).update().close();
        } catch (final Exception e) {
            //the pass died with its source; what matters is what the next reader sees
        }

        final MemoryFileMetadata metadata = lookup.getMemoryFileLookupTable().getMetadata();
        try (ICloseableProperties properties = metadata.getProperties()) {
            Assertions.checkTrue(metadata.getExpectedMemoryFileSize(properties)
                    != MemoryFileMetadata.MISSING_EXPECTED_MEMORY_FILE_SIZE,
                    "the completed pass attested a high-water, and the killed one must not have destroyed it");
            Assertions.checkNotNull(metadata.getExpectedMemoryFileGeneration(properties),
                    "and it is attested AS A PAIR - the unpaired form is the ambiguous state a current build never"
                            + " creates, whatever kills the JVM");
        }
        lookup.clearCaches();
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "the next reader must not be handed a store it has to wipe: " + lookup.getLastInconsistencyReason());
        Assertions.checkEquals(InconsistencyReason.NONE, lookup.getLastInconsistencyReason(),
                "no reason recorded for a killed pass");
        Assertions.checkTrue(readValues(table).size() >= landed.size(),
                "and the values the completed pass published stay readable");
    }

    /**
     * The residual the attempt-garbage lane named and left open: "an already-populated old store is not
     * retroactively fixed by this change". This pins what that actually costs. A store carrying the unpaired
     * attestation a pre-pairing build writes is indicted on every read, so the consumer wipes it - but the wipe is a
     * version increment followed by a pass that publishes, and a publish writes the generation beside the size. So
     * the store self-heals at its first rebuild, and the residue is one re-download, not a permanently un-wedgeable
     * key. Where the source can no longer serve the range, that re-download is the data loss - which is the honest
     * boundary of "the reset is conservative and correct": it is correct about the data it can still get.
     */
    @Test
    public void testAnUnpairedLegacyStoreSelfHealsAtItsFirstPublishAfterTheReset() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("legacySelfHeal");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        newUpdater(table, newValues(1, BATCH_FLUSH_INTERVAL + 2)).update().close();

        //strip the pairing, leaving the shape a pre-pairing build leaves behind: size only, and ahead of the chain
        final long highWater = readAttestation(lookup);
        final File propertiesFile = new File(new File(memoryDirectoryOf(lookup), "properties"), "memory.properties");
        java.nio.file.Files.write(propertiesFile.toPath(),
                ("EXPECTED_MEMORY_FILE_SIZE=" + (highWater + 1000L) + "\n").getBytes(Charsets.UTF_8));
        lookup.clearCaches();

        Assertions.checkTrue(lookup.isEmptyOrInconsistent(),
                "precondition: the unpaired store is indicted, exactly as the residual describes");
        Assertions.checkEquals(InconsistencyReason.HIGH_WATER_AHEAD_OF_CHAIN, lookup.getLastInconsistencyReason(),
                "by the high-water arm");

        //the consumer's remedy, then the pass that follows it
        table.deleteRange(KEY);
        lookup.clearCaches();
        Assertions.checkTrue(lookup.isEmptyOrInconsistent(),
                "an empty new version reports itself empty, which is what the demand side expects after a reset");
        final List<FDate> reSupplied = newValues(1, BATCH_FLUSH_INTERVAL * 2 + 2);
        newUpdater(table, reSupplied).update().close();
        lookup.clearCaches();

        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "after one publish the same store is no longer indicted - the pairing was rewritten by the publish");
        Assertions.checkEquals(InconsistencyReason.NONE, lookup.getLastInconsistencyReason(),
                "so the residual costs a rebuild, not a permanently wedged key");
        Assertions.checkEquals(reSupplied, readValues(table),
                "and the values the source re-supplied are the values the key now serves");
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

    private List<FDate> readValues(final ATimeSeriesDB<String, FDate> table) {
        final List<FDate> values = new ArrayList<>();
        try (ICloseableIterator<FDate> it = table.rangeValues(KEY, FDates.MIN_DATE, FDates.MAX_DATE).iterator()) {
            Lists.toListWithoutHasNext(it, values);
        }
        return values;
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

    /** One minute apart, so a fixture can ask for forty values without leaving the hour. */
    private static List<FDate> newValues(final int fromIndex, final int toIndex) {
        final List<FDate> values = new ArrayList<>();
        for (int i = fromIndex; i <= toIndex; i++) {
            values.add(FDateBuilder.newDate(2026, 1, 1, 0, i - 1, 0));
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
