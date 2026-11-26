package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
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
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.string.Charsets;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;

/**
 * A publish writes its byte high-water (EXPECTED_MEMORY_FILE_SIZE in properties/memory.properties) BEFORE the index
 * generation that high-water describes is renamed into place: the properties block of
 * {@link de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.VersionedTimeSeriesMemoryFileLookupTable#put}
 * closes inside the merge loop, while the generation publishes on the collection's close. A JVM killed in that window,
 * or a publish whose rename fails on I/O, therefore leaves an attestation naming a generation that never landed and a
 * high-water above the chain on disk.
 *
 * <p>
 * Read against the chain actually present, that pair used to answer {@link TimeSeriesLookupStorageCache#isEmptyOrInconsistent()}
 * with "inconsistent and needs to be reset" - and the derived-store consumers take that as licence to delete the key
 * and re-download it from the source, which past the source's serving window is data loss. The store's own length
 * check must not turn a crash that cost one publish into a crash that costs the key, so the high-water is paired with
 * the generation number it belongs to and the comparison applies only when they agree.
 *
 * <p>
 * Pinned from both sides, so the fix cannot be reached by dropping the comparison: an attestation of a generation that
 * is not the chain being read is skipped and the reads keep serving the intact prefix, while an attestation that DOES
 * describe the chain read (a store written before the pairing existed) still reports a chain that does not reach it.
 */
@NotThreadSafe
public class TimeSeriesAttestedGenerationCrashWindowTest extends ATest {

    private static final int BATCH_FLUSH_INTERVAL = 4;
    private static final String KEY = "attestedGenerationKey";

    private File baseDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY,
                TimeSeriesAttestedGenerationCrashWindowTest.class.getSimpleName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Files.deleteQuietly(baseDirectory);
    }

    /**
     * The state a kill between the two writes leaves: the attestation has moved to generation N+1, generation N is the
     * newest on disk, and nothing about the chain N itself is wrong. Producing it by publishing and then removing the
     * generation file is exactly what never renaming it looks like from the reader's side.
     */
    @Test
    public void testAnAttestationOfAGenerationThatNeverLandedDoesNotInvalidateTheChain() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("attestedCrashWindow");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        newUpdater(table, newValues(1, BATCH_FLUSH_INTERVAL + 2)).update().close();

        final List<MemoryFileSummary> published = readPublishedSummaries(lookup);
        Assertions.assertThat(published).as("the pass publishes a full batch and its short tail").hasSize(2);
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(), "the pass leaves a store its own check accepts");
        final int landedGeneration = lookup.getMemoryFileLookupTable().getCurrentIndexNumber();
        final long highWaterOfLandedChain = readExpectedMemoryFileSize(lookup);

        publishOneMoreSummary(lookup, published);
        final File interruptedGeneration = new File(memoryDirectoryOf(lookup),
                AMemoryFileSummarySerializingCollection.newIndexGenerationFileName(landedGeneration + 1));
        Assertions.checkTrue(interruptedGeneration.exists(),
                "the fixture publishes the generation the kill is supposed to have lost");
        Assertions.checkTrue(interruptedGeneration.delete(), "removing it must work: " + interruptedGeneration);
        lookup.clearCaches();

        final long attestedHighWater = readExpectedMemoryFileSize(lookup);
        Assertions.checkTrue(attestedHighWater > highWaterOfLandedChain,
                "the attestation must be ahead of the chain that landed: " + attestedHighWater + " vs "
                        + highWaterOfLandedChain);
        Assertions.assertThat(lookup.getMemoryFileLookupTable().getCurrentIndexNumber())
                .as("the newest generation present is still the one the previous pass landed")
                .isEqualTo(landedGeneration);

        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "an attestation of a generation that never landed must not report an intact key as damaged");
        final List<MemoryFileSummary> afterCrash = readPublishedSummaries(lookup);
        Assertions.assertThat(afterCrash).as("the reads keep serving the chain that landed").hasSize(published.size());
        Assertions.assertThat(extentOf(afterCrash.get(afterCrash.size() - 1)))
                .as("and its last summary is the same byte extent as before the interrupted publish")
                .isEqualTo(extentOf(published.get(published.size() - 1)));
    }

    /**
     * The other side of the pin: the comparison is not simply switched off. A store whose properties carry the
     * high-water without a generation key (written before the pairing) keeps the comparison it always had, so a chain
     * that does not reach its own attestation is still reported - and the fix must not be the deletion of the guard.
     */
    @Test
    public void testAnUnpairedAttestationStillReportsAChainThatDoesNotReachIt() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("attestedLegacyStore");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        newUpdater(table, newValues(1, BATCH_FLUSH_INTERVAL + 2)).update().close();
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(), "the pass leaves a consistent store");
        final long highWater = readExpectedMemoryFileSize(lookup);

        //java.nio is used deliberately: de.invesdwin.util.lang.Files writes no byte[]. The store's own format is a
        //plain properties file, and this is the shape a build predating the pairing leaves behind.
        final File propertiesFile = new File(new File(memoryDirectoryOf(lookup), "properties"), "memory.properties");
        Assertions.checkTrue(propertiesFile.exists(), "the publish writes its attestation there: " + propertiesFile);
        java.nio.file.Files.write(propertiesFile.toPath(),
                ("EXPECTED_MEMORY_FILE_SIZE=" + (highWater + 1000L) + "\n").getBytes(Charsets.UTF_8));
        lookup.clearCaches();

        Assertions.checkTrue(lookup.isEmptyOrInconsistent(),
                "a chain that does not reach its own attestation must still be reported as needing a reset");
    }

    /**
     * The pairing at the write site: a successful publish attests the high-water together with the generation number
     * it wrote, which is what lets the reader tell an interrupted publish from a damaged chain.
     */
    @Test
    public void testAPublishAttestsTheHighWaterWithTheGenerationItWrote() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("attestedPairing");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        newUpdater(table, newValues(1, BATCH_FLUSH_INTERVAL + 2)).update().close();

        final int currentIndexNumber = lookup.getMemoryFileLookupTable().getCurrentIndexNumber();
        Assertions.checkTrue(currentIndexNumber >= 0, "the pass published a generation");
        final MemoryFileMetadata metadata = lookup.getMemoryFileLookupTable().getMetadata();
        try (ICloseableProperties properties = metadata.getProperties()) {
            Assertions.assertThat(metadata.getExpectedMemoryFileGeneration(properties))
                    .as("the attestation names the generation whose chain it measured")
                    .isEqualTo(currentIndexNumber);
            Assertions.assertThat(metadata.getExpectedMemoryFileSize(properties))
                    .as("and the high-water is the extent of that chain's last summary")
                    .isPositive();
        }
    }

    // --- helpers ---

    private static File memoryDirectoryOf(final TimeSeriesLookupStorageCache<String, FDate> lookup) {
        return lookup.getDirectoryHashKeyVersionMemory().getDirectoryHashKeyVersionDataShared();
    }

    /**
     * Appends one summary to the published chain through the lookup table, which lands a new generation and moves the
     * attestation with it.
     */
    private static void publishOneMoreSummary(final TimeSeriesLookupStorageCache<String, FDate> lookup,
            final List<MemoryFileSummary> published) {
        final MemoryFileSummary last = published.get(published.size() - 1);
        final FDate laterValue = FDateBuilder.newDate(2020, 1, 1).addDays(BATCH_FLUSH_INTERVAL + 3);
        final long precedingMemoryOffset = last.getPrecedingMemoryOffset() + last.getMemoryOffset()
                + last.getMemoryLength();
        final long precedingValueCount = last.getPrecedingValueCount() + last.getValueCount();
        final MemoryFileSummary extra = new MemoryFileSummary(laterValue, FDateSerde.GET, laterValue, laterValue,
                precedingValueCount, 1, "memory_attestedcrashwindow.data", precedingMemoryOffset, 0L, 120L);
        lookup.getMemoryFileLookupTable().put(new SingleSummaryIterator(extra));
    }

    private static long extentOf(final MemoryFileSummary summary) {
        return summary.getPrecedingMemoryOffset() + summary.getMemoryOffset() + summary.getMemoryLength();
    }

    private static long readExpectedMemoryFileSize(final TimeSeriesLookupStorageCache<String, FDate> lookup) {
        final MemoryFileMetadata metadata = lookup.getMemoryFileLookupTable().getMetadata();
        try (ICloseableProperties properties = metadata.getProperties()) {
            return metadata.getExpectedMemoryFileSize(properties);
        }
    }

    private static List<MemoryFileSummary> readPublishedSummaries(
            final TimeSeriesLookupStorageCache<String, FDate> lookupTable) {
        final List<MemoryFileSummary> summaries = new ArrayList<>();
        try (ICloseableIterator<MemoryFileSummary> it = lookupTable.getMemoryFileLookupTable().range()) {
            Lists.toListWithoutHasNext(it, summaries);
        }
        return summaries;
    }

    private static List<FDate> newValues(final int fromDay, final int toDay) {
        final List<FDate> values = new ArrayList<>();
        for (int day = fromDay; day <= toDay; day++) {
            values.add(FDateBuilder.newDate(2020, 1, 1).addDays(day));
        }
        return values;
    }

    private static final class SingleSummaryIterator implements ICloseableIterator<MemoryFileSummary> {

        private final MemoryFileSummary summary;
        private boolean served;

        private SingleSummaryIterator(final MemoryFileSummary summary) {
            this.summary = summary;
        }

        @Override
        public boolean hasNext() {
            return !served;
        }

        @Override
        public MemoryFileSummary next() {
            if (served) {
                throw FastNoSuchElementException.getInstance("end reached");
            }
            served = true;
            return summary;
        }

        @Override
        public void close() {
            //nothing to release
        }
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
