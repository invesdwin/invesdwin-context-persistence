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

/**
 * The byte high-water a published generation carries (EXPECTED_MEMORY_FILE_SIZE in properties/memory.properties, the
 * number {@link TimeSeriesLookupStorageCache#isEmptyOrInconsistent()} later demands to equal both the chain-calculated
 * extent and the bytes on disk) is that generation's high-water, not a monotonically growing counter. Two halves, both
 * pinned here:
 * <ul>
 * <li>a new generation may legitimately end BELOW it when its first summary REPLACES the last summary of the previous
 * generation - the redo of the standalone '_incomplete_' last file, whose replacement region re-serializes to fewer
 * bytes than the file it replaces. Refusing that used to fail-loop the updater of the key: every retry redoes the same
 * file, re-produces the same shorter region and dies before publishing (the live signature is
 * 'prevMemoryFileSize[4193405] should be less than expectedMemoryFileFize[4193368]'). The expectation must follow the
 * chain down, or the store's own length check resets the key forever.</li>
 * <li>a summary that is NOT such a replacement may not fall back below the published high-water: that means its offsets
 * were computed against an older generation, and it stays refused - without the refusal moving the expectation.</li>
 * </ul>
 * The replacement is applied to the lookup table directly here, so the guard is exercised on a chain whose replacement
 * region really is shorter than the file it replaces.
 */
@NotThreadSafe
public class TimeSeriesPublishedChainHighWaterGuardTest extends ATest {

    private static final int BATCH_FLUSH_INTERVAL = 4;
    private static final String KEY = "chainHighWaterKey";

    private File baseDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY,
                TimeSeriesPublishedChainHighWaterGuardTest.class.getSimpleName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Files.deleteQuietly(baseDirectory);
    }

    /**
     * Minimal shape: a published complete batch plus a standalone '_incomplete_' last file; the replacement of that
     * last file re-serializes shorter and must be published, with the expectation following it down and the store
     * still reporting itself consistent.
     */
    @Test
    public void testReplacementSummaryWithALowerExtentIsPublished() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("chainHighWaterReplace");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        publishReplacementWithLowerExtent(table, lookup, newValues(1, BATCH_FLUSH_INTERVAL + 2));
    }

    /**
     * The shape a live key showed with the resumable rails: a boundary commit publishes and rotates the writer onto a
     * fresh file, the attempt lands its last short segment in the standalone '_incomplete_' file, and the replacement
     * of that file is shorter. The refusal then fires at the final publish, which is where the live fail-loop died.
     */
    @Test
    public void testReplacementAfterABoundaryCommitIsPublished() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("chainHighWaterReplaceBoundary");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final int boundaryValues = TimeSeriesProperties.UPDATE_COMMIT_SEGMENT_INTERVAL * BATCH_FLUSH_INTERVAL;
        Assertions.checkTrue(lookup.getMemoryFileLookupTable().getCurrentIndexNumber() == -1,
                "the key starts without an index generation");
        publishReplacementWithLowerExtent(table, lookup, newValues(1, boundaryValues + 2));
        Assertions.checkTrue(lookup.getMemoryFileLookupTable().getCurrentIndexNumber() >= 2,
                "a boundary commit and the replacement mean at least three generations");
    }

    /**
     * The other half of the invariant: a summary that is NOT the replacement of the last published file and whose byte
     * range falls back below the published high-water is a byte space regression and stays refused, and the refusal
     * must not move the expectation.
     */
    /**
     * The whole point of the delta chain: inside one attempt, every boundary-commit publish CONTINUES the chain, so it
     * writes only its own records and the base stays the attempt's first generation. The live shape this bounds is 781
     * generations per attempt on the worst ptstaging key (measured 2026-11-14 from lap-mid over ssh) - if each of those
     * publishes rewrote the chain, the layout would buy nothing and the index would keep costing ~54 % of the key.
     */
    @Test
    public void testABoundaryCommitPassAppendsInsteadOfRewritingTheChain() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("chainAppendBoundary");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        //three boundary-commit intervals worth of values, so the pass publishes several generations
        final int boundaryValues = 3 * TimeSeriesProperties.UPDATE_COMMIT_SEGMENT_INTERVAL * BATCH_FLUSH_INTERVAL;
        newUpdater(table, newValues(1, boundaryValues + 2)).update().close();

        final int newestGeneration = lookup.getMemoryFileLookupTable().getCurrentIndexNumber();
        Assertions.checkTrue(newestGeneration >= 2,
                "the scenario needs a pass that published several generations by boundary commits, got: "
                        + newestGeneration);
        final MemoryFileMetadata metadata = lookup.getMemoryFileLookupTable().getMetadata();
        try (ICloseableProperties properties = metadata.getProperties()) {
            Assertions.assertThat(metadata.getChainBase(properties))
                    .as("a continuing publish appends, so the base stays the first generation of the pass")
                    .isZero();
        }
        final List<MemoryFileSummary> published = readPublishedSummaries(lookup);
        Assertions.assertThat(published).as("the assembled chain still holds every record of the pass").hasSize(
                (boundaryValues + 2 + BATCH_FLUSH_INTERVAL - 1) / BATCH_FLUSH_INTERVAL);
        assertPublishedBytesMatchNamedFiles(lookup, published);
    }

    @Test
    public void testNonReplacingByteSpaceRegressionIsStillRefused() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("chainHighWaterRegression");
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
     * Publishes a chain, then replaces its last summary with one that claims FEWER bytes for the same position (backed
     * by a data file carrying exactly those bytes) and asserts the publish landed: the expectation followed the chain
     * down, the chain is contiguous and every named file still carries exactly its published bytes.
     */
    private void publishReplacementWithLowerExtent(final ATimeSeriesDB<String, FDate> table,
            final TimeSeriesLookupStorageCache<String, FDate> lookup, final List<FDate> firstPass) throws Exception {
        newUpdater(table, firstPass).update().close();
        final List<MemoryFileSummary> publishedFirst = readPublishedSummaries(lookup);
        final MemoryFileSummary replaced = publishedFirst.get(publishedFirst.size() - 1);
        Assertions.checkTrue(MemoryFiles.isIncompleteMemoryFile(replaced.getMemoryResourceUri()),
                "the scenario needs a published '_incomplete_' last file: " + replaced);
        final long highWaterFirst = readExpectedMemoryFileSize(lookup);
        Assertions.assertThat(highWaterFirst).as("pass 1 byte high-water").isEqualTo(extentOf(replaced));

        //the same period, re-serialized shorter than the file it replaces
        final long replacedLength = replaced.getMemoryLength();
        final long shrunkLength = replacedLength - 10;
        final File replacedFile = new File(replaced.getMemoryResourceUri());
        final File replacementFile = new File(replacedFile.getParentFile(),
                "replacement_shorter_of_" + replacedFile.getName());
        //java.nio is used deliberately: de.invesdwin.util.lang.Files is imported here and writes no byte[]
        java.nio.file.Files.write(replacementFile.toPath(), new byte[(int) shrunkLength]);
        final MemoryFileSummary replacement = new MemoryFileSummary(replaced.getFirstValueEndTime(), FDateSerde.GET,
                firstPass.get(firstPass.size() - 2), firstPass.get(firstPass.size() - 1),
                replaced.getPrecedingValueCount(), replaced.getValueCount(), replacementFile.getAbsolutePath(),
                replaced.getPrecedingMemoryOffset(), replaced.getMemoryOffset(), shrunkLength);

        lookup.getMemoryFileLookupTable().put(new SingleSummaryIterator(replacement));

        final long highWaterSecond = readExpectedMemoryFileSize(lookup);
        Assertions.assertThat(highWaterSecond).as("expectation after a legitimately shorter replacement")
                .isLessThan(highWaterFirst);
        Assertions.assertThat(highWaterSecond).as("the metadata expectation must follow the published chain")
                .isEqualTo(extentOf(replacement));
        final List<MemoryFileSummary> publishedSecond = readPublishedSummaries(lookup);
        Assertions.assertThat(publishedSecond).as("summaries after the replacement")
                .hasSize(publishedFirst.size());
        Assertions.checkNotEquals(publishedSecond.get(publishedSecond.size() - 1).getMemoryResourceUri(),
                replaced.getMemoryResourceUri(), "the replacement must name a data file of its own");
        assertPublishedBytesMatchNamedFiles(lookup, publishedSecond);
        Assertions.checkTrue(replacedFile.exists(), "a replacement must not delete the file it replaces: "
                + replacedFile);
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
