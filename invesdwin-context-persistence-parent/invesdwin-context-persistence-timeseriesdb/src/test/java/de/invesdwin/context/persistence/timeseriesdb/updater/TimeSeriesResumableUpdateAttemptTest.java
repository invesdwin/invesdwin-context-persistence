package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.ITimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFiles;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdateProgress;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdaterInternalMethods;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.SequentialChunkedUpdateProgress;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.collections.iterable.skip.ASkippingIterable;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FDates;

/**
 * An update attempt that dies mid-pass (a source that times out or is cancelled) must not throw away the bytes it has
 * already fetched. The publish unit is the segment: every {@link TimeSeriesProperties#UPDATE_COMMIT_SEGMENT_INTERVAL}
 * completed segments the attempt publishes what it has staged so far as a new index generation, so the aborted
 * attempt leaves a readable high-water mark behind and the next attempt of the same key continues from it instead of
 * restarting at byte space 0. Everything the aborted attempt staged after its last published boundary is discarded,
 * and the data files that no published generation names are reclaimed - no '_incomplete_' litter accumulating.
 */
@NotThreadSafe
public class TimeSeriesResumableUpdateAttemptTest extends ATest {

    private static final int BATCH_FLUSH_INTERVAL = 4;
    private static final String KEY = "resumableKey";

    private File baseDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY,
                TimeSeriesResumableUpdateAttemptTest.class.getSimpleName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Files.deleteQuietly(baseDirectory);
    }

    @Test
    public void testAbortedAttemptResumesFromPublishedHighWater() throws Exception {
        checkAbortedAttemptResumes(false);
    }

    @Test
    public void testAbortedAttemptResumesFromPublishedHighWaterInParallel() throws Exception {
        checkAbortedAttemptResumes(true);
    }

    private void checkAbortedAttemptResumes(final boolean parallel) throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("resume" + parallel);
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final int commitInterval = TimeSeriesProperties.UPDATE_COMMIT_SEGMENT_INTERVAL;
        //a parallel writer feeds its queue ahead of the writer loop, so the pass has to die well after the first
        //boundary commit for that boundary to be published when the source dies
        final int firstBoundaryValues = commitInterval * BATCH_FLUSH_INTERVAL;
        final int landedValues = 3 * firstBoundaryValues + BATCH_FLUSH_INTERVAL;
        final int totalValues = landedValues + BATCH_FLUSH_INTERVAL;
        final List<FDate> allValues = newValues(1, totalValues);

        final ATimeSeriesUpdater<String, FDate> aborting = newUpdater(table, KEY,
                abortingSource(allValues, landedValues), parallel);
        boolean aborted = false;
        try {
            aborting.update().close();
        } catch (final IncompleteUpdateRetryableException e) {
            aborted = true;
        }
        Assertions.checkTrue(aborted, "the aborted pass must propagate");
        final String abortedAttemptId = aborting.getEpochId();
        Assertions.checkNotNull(abortedAttemptId, "the aborted attempt must have minted its data file id");

        //the segments up to the last boundary commit are published: their bytes are not lost, and what the attempt
        //staged after its last publish is not
        final List<MemoryFileSummary> published = readPublishedSummaries(lookup);
        Assertions.checkTrue(!published.isEmpty(),
                "the boundary commit must have published the segments it completed before the abort");
        final List<FDate> publishedValues = readValues(table, KEY);
        Assertions.checkTrue(publishedValues.size() >= firstBoundaryValues,
                "at least one boundary must be readable after the abort, got: " + publishedValues.size());
        Assertions.checkTrue(publishedValues.size() < landedValues,
                "what the attempt staged after its last publish must not be published: " + publishedValues.size());
        Assertions.assertThat(publishedValues).isEqualTo(new ArrayList<>(
                allValues.subList(0, publishedValues.size())));
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "the store must not report itself inconsistent after an aborted attempt: " + published);

        //the files of the aborted attempt that survive are exactly the ones the published generation names: the
        //staged-but-unpublished tail of the attempt leaves no litter behind
        final Set<String> referencedNames = ILockCollectionFactory.getInstance(false).newLinkedSet();
        for (final MemoryFileSummary summary : published) {
            referencedNames.add(new File(summary.getMemoryResourceUri()).getName());
        }
        for (final File leftover : findAttemptDataFiles(lookup, abortedAttemptId)) {
            Assertions.checkTrue(referencedNames.contains(leftover.getName()),
                    "the aborted attempt left an unreferenced data file behind: " + leftover);
            Assertions.checkFalse(MemoryFiles.isIncompleteMemoryFile(leftover.getName()),
                    "an aborted attempt must not leave '_incomplete_' litter behind: " + leftover);
        }
        final byte[] publishedBytes = readAllBytes(new File(published.get(0).getMemoryResourceUri()));

        //the next attempt continues where the aborted one stopped: the values it re-fetches are the ones that were
        //never published, nothing published before is rewritten or lost
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(allValues), parallel).update().close();
        Assertions.assertThat(readValues(table, KEY)).isEqualTo(new ArrayList<>(allValues));
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "the store must stay consistent over the resume: " + readPublishedSummaries(lookup));
        final List<MemoryFileSummary> resumed = readPublishedSummaries(lookup);
        for (int i = 0; i < published.size(); i++) {
            Assertions.checkEquals(published.get(i), resumed.get(i),
                    i + ": the resume must not change an already published summary");
        }
        final File firstPublishedFile = new File(published.get(0).getMemoryResourceUri());
        Assertions.checkTrue(firstPublishedFile.exists(), "the resume removed a published data file: "
                + firstPublishedFile);
        Assertions.checkTrue(Arrays.equals(publishedBytes, readAllBytes(firstPublishedFile)),
                "the resume mutated the bytes of a published data file: " + firstPublishedFile);
    }

    /**
     * A transaction closed without a final commit discards only what it staged after its last published boundary; the
     * boundary published before stays visible, and continuing the store with a new attempt lands every value exactly
     * once.
     */
    @Test
    public void testCloseWithoutCommitKeepsOnlyPublishedBoundaries() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("boundaryCommit");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final int commitInterval = TimeSeriesProperties.UPDATE_COMMIT_SEGMENT_INTERVAL;
        final int firstBoundaryValues = commitInterval * BATCH_FLUSH_INTERVAL;
        final int landedValues = 2 * firstBoundaryValues + BATCH_FLUSH_INTERVAL;
        final int totalValues = landedValues + BATCH_FLUSH_INTERVAL;
        final List<FDate> allValues = newValues(1, totalValues);

        final TimeSeriesUpdateTransaction<FDate> transaction = lookup.newUpdateTransaction(true);
        boolean aborted = false;
        try {
            SequentialChunkedUpdateProgress.doUpdate(transaction,
                    new UpdatingInternalMethods(table, lookup, transaction.getEpochLocalDataId()),
                    transaction.getPrecedingMemoryOffset(), transaction.getMemoryOffset(),
                    transaction.getPrecedingValueCount(), skippingPublished(transaction,
                            abortingSource(allValues, landedValues)));
        } catch (final IllegalStateException e) {
            aborted = true;
        } finally {
            transaction.close();
        }
        Assertions.checkTrue(aborted, "the aborting source must propagate out of the writer");

        final List<MemoryFileSummary> published = readPublishedSummaries(lookup);
        Assertions.checkTrue(!published.isEmpty(),
                "the segments published at the boundary must survive a close without commit");
        final List<FDate> publishedValues = readValues(table, KEY);
        Assertions.checkTrue(publishedValues.size() >= firstBoundaryValues,
                "at least one boundary must be readable, got: " + publishedValues.size());
        Assertions.assertThat(publishedValues).isEqualTo(new ArrayList<>(
                allValues.subList(0, publishedValues.size())));
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "the published boundary must be a consistent store: " + published);

        //the tail the same transaction staged after its last boundary is dropped, so continuing the store with a new
        //attempt lands every value exactly once
        final TimeSeriesUpdateTransaction<FDate> next = lookup.newUpdateTransaction(true);
        try {
            SequentialChunkedUpdateProgress.doUpdate(next,
                    new UpdatingInternalMethods(table, lookup, next.getEpochLocalDataId()),
                    next.getPrecedingMemoryOffset(), next.getMemoryOffset(), next.getPrecedingValueCount(),
                    skippingPublished(next, WrapperCloseableIterable.maybeWrap(allValues)));
            next.commit();
        } finally {
            next.close();
        }
        Assertions.assertThat(readValues(table, KEY)).isEqualTo(new ArrayList<>(allValues));
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "the store must stay consistent after the tail was published: " + readPublishedSummaries(lookup));
    }

    /**
     * The same guard {@link ATimeSeriesUpdater} puts in front of every attempt's source: an attempt that continues a
     * published boundary must not write the values the store already holds a second time.
     */
    private static ICloseableIterable<FDate> skippingPublished(final TimeSeriesUpdateTransaction<FDate> transaction,
            final ICloseableIterable<FDate> source) {
        final FDate resumeAfterEndTime = transaction.getResumeAfterEndTime();
        if (resumeAfterEndTime == null) {
            return source;
        }
        return new ASkippingIterable<FDate>(source) {
            @Override
            protected boolean skip(final FDate element) {
                return !element.isAfterNotNullSafe(resumeAfterEndTime);
            }
        };
    }

    /**
     * A source that hands out {@code landed} values and then throws - the way a stream that times out or gets
     * cancelled kills the pass. It paces itself like a real feed (the parallel writer's producer would otherwise race
     * far ahead of the writer loop and the pass would die before it ever wrote a segment): values are handed out at
     * about a millisecond each.
     */
    private static ICloseableIterable<FDate> abortingSource(final List<FDate> values, final int landed) {
        return new ICloseableIterable<FDate>() {
            @Override
            public ICloseableIterator<FDate> iterator() {
                return new ICloseableIterator<FDate>() {

                    private int index = 0;

                    @Override
                    public boolean hasNext() {
                        return true;
                    }

                    @Override
                    public FDate next() {
                        if (index >= landed) {
                            throw new IllegalStateException(
                                    "abort mid-pass after " + landed + " fetched values");
                        }
                        Duration.ONE_MILLISECOND.sleepNoInterrupt();
                        return values.get(index++);
                    }

                    @Override
                    public void close() {}
                };
            }
        };
    }

    private static List<File> findAttemptDataFiles(final TimeSeriesLookupStorageCache<String, FDate> lookup,
            final String attemptId) {
        final File dataDirectory = lookup.getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        final File[] files = dataDirectory
                .listFiles((dir, name) -> name.startsWith(MemoryFiles.EPOCH_LOCAL_DATA_FILE_PREFIX + attemptId));
        final List<File> found = new ArrayList<>();
        if (files != null) {
            for (final File file : files) {
                found.add(file);
            }
        }
        return found;
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

    /**
     * An update attempt that states the epoch local data id of its transaction explicitly, the way
     * {@link ATimeSeriesUpdater} does it through its update transaction.
     */
    @NotThreadSafe
    private static final class UpdatingInternalMethods
            implements ITimeSeriesUpdaterInternalMethods<String, FDate> {

        private final ATimeSeriesDB<String, FDate> table;
        private final TimeSeriesLookupStorageCache<String, FDate> lookupTable;
        private final String epochLocalDataId;

        private UpdatingInternalMethods(final ATimeSeriesDB<String, FDate> table,
                final TimeSeriesLookupStorageCache<String, FDate> lookupTable, final String epochLocalDataId) {
            this.table = table;
            this.lookupTable = lookupTable;
            this.epochLocalDataId = epochLocalDataId;
        }

        @Override
        public ISerde<FDate> getValueSerde() {
            return table.getValueSerde();
        }

        @Override
        public TimeSeriesLookupStorageCache<String, FDate> getLookupTable() {
            return lookupTable;
        }

        @Override
        public ITimeSeriesDB<String, FDate> getTable() {
            return table;
        }

        @Override
        public FDate extractStartTime(final FDate element) {
            return element;
        }

        @Override
        public FDate extractEndTime(final FDate element) {
            return element;
        }

        @Override
        public void onElement(final ITimeSeriesUpdateProgress updateProgress, final long pendingCount) {}

        @Override
        public void onFlush(final ITimeSeriesUpdateProgress updateProgress, final long flushIndex) {}

        @Override
        public String getKey() {
            return KEY;
        }

        @Override
        public boolean shouldRedoLastFile() {
            //the default of an incremental updater: its last batch lands in an incomplete file that the next attempt
            //redoes
            return true;
        }

        @Override
        public String getEpochLocalDataId() {
            return epochLocalDataId;
        }
    }

    /**
     * java.nio is spelled out because this file imports the invesdwin {@link Files} helper for the store directories.
     */
    private static byte[] readAllBytes(final File file) throws Exception {
        return java.nio.file.Files.readAllBytes(file.toPath());
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

    private ATimeSeriesUpdater<String, FDate> newUpdater(final ATimeSeriesDB<String, FDate> table, final String key,
            final ICloseableIterable<? extends FDate> source, final boolean parallel) {
        return new ATimeSeriesUpdater<String, FDate>(key, table) {

            @Override
            protected ICloseableIterable<? extends FDate> getSource(final FDate updateFrom) {
                return source;
            }

            @Override
            protected boolean shouldWriteInParallel() {
                return parallel;
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
