package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFiles;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.MemoryFileMetadata;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdateProgress;
import de.invesdwin.context.system.properties.ICloseableProperties;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.Longs;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FDates;

/**
 * The redo of an incomplete last file may legitimately publish FEWER bytes than the file it replaces: the redo drops
 * the last value of that file (it might be an incomplete bar) and a source that no longer hands that value back leaves
 * the replaced region shorter than the discarded file was. That is a shrink of the store's byte high-water, not a
 * corruption, and the publish must land: the new index generation names the shorter file, the metadata expectation
 * follows the published chain down to the new high-water, and the store keeps reporting itself consistent.
 * <p>
 * The lookup table used to read its persisted expectation as monotonically growing and refused every lower value with
 * an IllegalStateException, which fail-loops the updater of such a key forever - each retry redoes the same file,
 * re-produces the same shorter region and dies again before publishing anything. The live signature is
 * 'prevMemoryFileSize[4193405] should be less than expectedMemoryFileFize[4193368]': 37 bytes, one one-minute bar's
 * worth of compressed bytes, and the published files on disk carry exactly their summaries' bytes (the overhang is in
 * the re-derived extent of the redo, not in the data files). The refusal is only right when the incoming summary does
 * NOT replace the last published file, so the last case pins that half of the invariant: a byte space regression that
 * is not a replacement stays refused.
 */
@NotThreadSafe
public class TimeSeriesRedoShrinkingLastFileTest extends ATest {

    private static final int BATCH_FLUSH_INTERVAL = 4;
    private static final String KEY = "shrinkingRedoKey";

    private File baseDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY,
                TimeSeriesRedoShrinkingLastFileTest.class.getSimpleName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Files.deleteQuietly(baseDirectory);
    }

    /**
     * The minimal shape: pass 1 publishes one complete batch plus a standalone '_incomplete_' file of two values; pass
     * 2 redoes that file and its source no longer serves the value the redo dropped, so the replacement holds one value
     * and strictly fewer bytes. The publish must land and the store must read the shorter chain.
     */
    @Test
    public void testRedoPublishesTheShorterLastFileInsteadOfFailing() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("shrinkingRedo");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final List<FDate> firstPass = newValues(1, BATCH_FLUSH_INTERVAL + 2);
        newUpdater(table, firstPass).update().close();

        final List<MemoryFileSummary> publishedFirst = readPublishedSummaries(lookup);
        Assertions.assertThat(publishedFirst).as("pass 1 summaries").hasSize(2);
        final MemoryFileSummary publishedLast = publishedFirst.get(publishedFirst.size() - 1);
        Assertions.checkTrue(MemoryFiles.isIncompleteMemoryFile(publishedLast.getMemoryResourceUri()),
                "the scenario needs a published '_incomplete_' last file: " + publishedLast);
        final long firstHighWater = readExpectedMemoryFileSize(lookup);
        Assertions.assertThat(firstHighWater).as("pass 1 byte high-water").isEqualTo(extentOf(publishedLast));
        final File replacedFile = new File(publishedLast.getMemoryResourceUri());
        final byte[] replacedBytes = readAllBytes(replacedFile);

        //pass 2: the supply shrank, the value the redo drops is not served again
        newUpdater(table, firstPass.subList(0, firstPass.size() - 1)).update().close();

        final List<MemoryFileSummary> publishedSecond = readPublishedSummaries(lookup);
        Assertions.assertThat(publishedSecond).as("pass 2 summaries").hasSize(2);
        final MemoryFileSummary republishedLast = publishedSecond.get(publishedSecond.size() - 1);
        Assertions.checkNotEquals(republishedLast.getMemoryResourceUri(), publishedLast.getMemoryResourceUri(),
                "the redo must name a data file of its own attempt");
        Assertions.checkTrue(republishedLast.getMemoryLength() < publishedLast.getMemoryLength(),
                "the redo must carry fewer bytes than the file it replaces: " + publishedLast + " vs "
                        + republishedLast);
        final long secondHighWater = readExpectedMemoryFileSize(lookup);
        Assertions.assertThat(secondHighWater).as("pass 2 byte high-water after the shrink")
                .isLessThan(firstHighWater);
        Assertions.assertThat(secondHighWater).as("the metadata expectation must follow the published chain")
                .isEqualTo(extentOf(republishedLast));
        assertPublishedBytesMatchNamedFiles(lookup, publishedSecond);
        Assertions.checkTrue(replacedFile.exists(), "the redo must not delete a previously published file: "
                + replacedFile);
        Assertions.checkTrue(Arrays.equals(replacedBytes, readAllBytes(replacedFile)),
                "the redo must not mutate bytes of the file it replaces: " + replacedFile);
        Assertions.assertThat(readValues(table, KEY)).as("readable content of the shorter chain")
                .isEqualTo(new ArrayList<>(firstPass.subList(0, firstPass.size() - 1)));
    }

    /**
     * The shape a live key showed with the resumable rails in place: a boundary commit publishes and rotates the writer
     * onto a fresh file, the attempt then lands its last (short) segment in a standalone '_incomplete_' file, and the
     * NEXT attempt's redo of that file produces fewer bytes - the refusal then fires at the final publish (a committed
     * close), which is exactly where the live fail-loop died, 1620 times on one key.
     */
    @Test
    public void testRedoAfterBoundaryCommitPublishesTheShorterLastFile() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("shrinkingRedoBoundary");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final int boundaryValues = TimeSeriesProperties.UPDATE_COMMIT_SEGMENT_INTERVAL * BATCH_FLUSH_INTERVAL;
        final List<FDate> firstPass = newValues(1, boundaryValues + 2);
        newUpdater(table, firstPass).update().close();
        Assertions.checkTrue(lookup.getMemoryFileLookupTable().getCurrentIndexNumber() >= 1,
                "a boundary commit must have published a generation before the final one");

        final List<MemoryFileSummary> publishedFirst = readPublishedSummaries(lookup);
        Assertions.assertThat(publishedFirst).as("pass 1 summaries")
                .hasSize(TimeSeriesProperties.UPDATE_COMMIT_SEGMENT_INTERVAL + 1);
        final MemoryFileSummary publishedLast = publishedFirst.get(publishedFirst.size() - 1);
        Assertions.checkTrue(MemoryFiles.isIncompleteMemoryFile(publishedLast.getMemoryResourceUri()),
                "the scenario needs a published '_incomplete_' last file: " + publishedLast);
        final long firstHighWater = readExpectedMemoryFileSize(lookup);
        Assertions.assertThat(firstHighWater).as("pass 1 byte high-water").isEqualTo(extentOf(publishedLast));

        newUpdater(table, firstPass.subList(0, firstPass.size() - 1)).update().close();

        final List<MemoryFileSummary> publishedSecond = readPublishedSummaries(lookup);
        final MemoryFileSummary republishedLast = publishedSecond.get(publishedSecond.size() - 1);
        Assertions.checkTrue(republishedLast.getMemoryLength() < publishedLast.getMemoryLength(),
                "the redo must carry fewer bytes than the file it replaces: " + publishedLast + " vs "
                        + republishedLast);
        final long secondHighWater = readExpectedMemoryFileSize(lookup);
        Assertions.assertThat(secondHighWater).as("pass 2 byte high-water after the shrink")
                .isLessThan(firstHighWater);
        Assertions.assertThat(secondHighWater).as("the metadata expectation must follow the published chain")
                .isEqualTo(extentOf(republishedLast));
        assertPublishedBytesMatchNamedFiles(lookup, publishedSecond);
        Assertions.assertThat(readValues(table, KEY)).as("readable content of the shorter chain")
                .isEqualTo(new ArrayList<>(firstPass.subList(0, firstPass.size() - 1)));
    }

    /**
     * The other half of the invariant, unchanged: a summary that is NOT the replacement of the last published file may
     * not fall back below the published byte high-water even when it stays above the chain's second-to-last extent -
     * such a byte range means the attempt computed its offsets against an older generation, and it stays refused.
     */
    @Test
    public void testNonReplacingByteSpaceRegressionIsStillRefused() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("shrinkingRedoRegression");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final List<FDate> values = newValues(1, BATCH_FLUSH_INTERVAL + 2);
        newUpdater(table, values).update().close();
        final List<MemoryFileSummary> published = readPublishedSummaries(lookup);
        Assertions.assertThat(published).as("published summaries").hasSize(2);
        final MemoryFileSummary publishedLast = published.get(published.size() - 1);
        final long highWater = readExpectedMemoryFileSize(lookup);
        Assertions.assertThat(highWater).as("published byte high-water").isEqualTo(extentOf(publishedLast));
        final long secondLastExtent = extentOf(published.get(published.size() - 2));
        Assertions.checkTrue(secondLastExtent < highWater,
                "the scenario needs a high-water above the second-to-last extent: " + published);

        //a summary of a later period whose byte range restarts near the start of the chain: above the extent of the
        //chain below the last summary, but below the published high-water - not a replacement, a regression
        final FDate regressingValue = newValues(BATCH_FLUSH_INTERVAL + 3, BATCH_FLUSH_INTERVAL + 3).get(0);
        final long regressingLength = secondLastExtent + 1;
        Assertions.checkTrue(regressingLength < highWater,
                "the regressing range must stay below the published high-water: " + regressingLength + " vs "
                        + highWater);
        final MemoryFileSummary regressing = new MemoryFileSummary(regressingValue, FDateSerde.GET, regressingValue,
                regressingValue, 0L, 1, publishedLast.getMemoryResourceUri(), 0L, 0L, regressingLength);
        try {
            lookup.getMemoryFileLookupTable().put(new SingleSummaryIterator(regressing));
            Assertions.fail("a non-replacing byte space regression must still be refused");
        } catch (final IllegalStateException e) {
            Assertions.checkTrue(e.getMessage().contains("prevMemoryFileSize"),
                    "the refusal must name the byte high-water guard: " + e.getMessage());
        }
        Assertions.assertThat(readExpectedMemoryFileSize(lookup)).as("the refusal must not move the high-water")
                .isEqualTo(highWater);
    }

    /**
     * Read off the store the way its own length check reads it: the published chain is contiguous and every data file a
     * published generation names carries exactly the bytes the summaries naming it claim - no byte beyond, no byte
     * missing.
     */
    private static void assertPublishedBytesMatchNamedFiles(final TimeSeriesLookupStorageCache<String, FDate> lookup,
            final List<MemoryFileSummary> published) {
        Assertions.checkTrue(!published.isEmpty(), "the store must have published something");
        long totalPublishedBytes = 0L;
        long highestClaimedByte = 0L;
        for (final MemoryFileSummary summary : published) {
            Assertions.assertThat(summary.getMemoryLength()).as("published length of " + summary).isGreaterThan(0L);
            totalPublishedBytes += summary.getMemoryLength();
            highestClaimedByte = Longs.max(highestClaimedByte, extentOf(summary));
        }
        Assertions.assertThat(highestClaimedByte).as("the published byte chain must be contiguous")
                .isEqualTo(totalPublishedBytes);

        final List<File> namedFiles = new ArrayList<>();
        for (final MemoryFileSummary summary : published) {
            final File named = new File(summary.getMemoryResourceUri());
            if (!namedFiles.contains(named)) {
                namedFiles.add(named);
            }
        }
        long totalBytesOnDisk = 0L;
        for (final File namedFile : namedFiles) {
            long publishedBytesOfThisFile = 0L;
            for (final MemoryFileSummary summary : published) {
                if (namedFile.getAbsolutePath().equals(summary.getMemoryResourceUri())) {
                    publishedBytesOfThisFile += summary.getMemoryLength();
                }
            }
            Assertions.checkTrue(namedFile.exists(), "a published generation names a missing file: " + namedFile);
            Assertions.assertThat(namedFile.length()).as("bytes on disk of " + namedFile)
                    .isEqualTo(publishedBytesOfThisFile);
            totalBytesOnDisk += namedFile.length();
        }
        Assertions.assertThat(totalBytesOnDisk).as("every published byte must be on disk, and no byte beyond")
                .isEqualTo(totalPublishedBytes);
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "the store's own length check must pass: " + published);
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

    private static byte[] readAllBytes(final File file) throws Exception {
        //java.nio is used deliberately: de.invesdwin.util.lang.Files is imported here and has no byte[] read
        return java.nio.file.Files.readAllBytes(file.toPath());
    }

    private static List<FDate> newValues(final int fromDay, final int toDay) {
        final List<FDate> values = new ArrayList<>();
        for (int day = fromDay; day <= toDay; day++) {
            values.add(FDateBuilder.newDate(2020, 1, 1).addDays(day));
        }
        return values;
    }

    private static List<MemoryFileSummary> readPublishedSummaries(
            final TimeSeriesLookupStorageCache<String, FDate> lookupTable) {
        final List<MemoryFileSummary> summaries = new ArrayList<>();
        try (ICloseableIterator<MemoryFileSummary> it = lookupTable.getMemoryFileLookupTable().range()) {
            Lists.toListWithoutHasNext(it, summaries);
        }
        return summaries;
    }

    private static List<FDate> readValues(final ATimeSeriesDB<String, FDate> table, final String key) {
        final List<FDate> values = new ArrayList<>();
        try (ICloseableIterator<FDate> it = table.rangeValues(key, FDates.MIN_DATE, FDates.MAX_DATE).iterator()) {
            Lists.toListWithoutHasNext(it, values);
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
        public void close() {}
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
