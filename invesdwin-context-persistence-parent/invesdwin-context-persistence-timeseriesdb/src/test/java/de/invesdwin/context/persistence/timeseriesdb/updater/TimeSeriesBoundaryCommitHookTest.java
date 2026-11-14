package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.ITimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdateProgress;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdaterInternalMethods;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.SequentialChunkedUpdateProgress;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;

/**
 * The boundary-commit hook of the resumable update rails (5974d85c, publish-then-yield cca6cf29): every boundary
 * commit that PUBLISHES notifies an attached {@link ITimeSeriesBoundaryCommitListener} with the newly published
 * frontier - the end time of the last published value. Consumers above the store (the trading caching services,
 * which keep the downloaded first/last date frontier keys) advance their view with each published segment instead
 * of only after a complete update; without the hook, an attempt that dies or yields mid-pass leaves the published
 * prefix invisible one layer up, and the next attempt rebases over it (cap25/26 attempt 5: the frontier gate read
 * a null frontier forever because the keys were only ever written after a doUpdate that never completed).
 * The hook fires only for calls that actually published - a boundary call below the commit interval publishes
 * nothing and notifies nothing.
 */
@NotThreadSafe
public class TimeSeriesBoundaryCommitHookTest extends ATest {

    private static final int BATCH_FLUSH_INTERVAL = 4;
    private static final String KEY = "hookKey";

    private File baseDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY,
                TimeSeriesBoundaryCommitHookTest.class.getSimpleName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Files.deleteQuietly(baseDirectory);
    }

    @Test
    public void testBoundaryCommitListenerSeesEveryPublishedFrontier() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("hookBoundary");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final int commitInterval = TimeSeriesUpdateTransaction.PUBLISH_SEGMENTS_PER_GENERATION;
        final int firstBoundaryValues = commitInterval * BATCH_FLUSH_INTERVAL;
        final List<FDate> frontiers = new ArrayList<>();
        final TimeSeriesUpdateTransaction<FDate> transaction = lookup.newUpdateTransaction(true);
        transaction.setBoundaryCommitListener(new ITimeSeriesBoundaryCommitListener() {

            @Override
            public void onBoundaryCommitPublished(final FDate publishedEndTime) {
                frontiers.add(publishedEndTime);
            }
        });
        //the pass lands one segment past the first boundary and dies there: the first boundary published, so
        //the listener must have seen exactly its frontier
        final int landedValues = firstBoundaryValues + BATCH_FLUSH_INTERVAL;
        final List<FDate> allValues = newValues(1, 2 * firstBoundaryValues);
        boolean aborted = false;
        try {
            SequentialChunkedUpdateProgress.doUpdate(transaction,
                    new UpdatingInternalMethods(table, lookup, transaction.getEpochLocalDataId()),
                    transaction.getPrecedingMemoryOffset(), transaction.getMemoryOffset(),
                    transaction.getPrecedingValueCount(), abortingSource(allValues, landedValues));
        } catch (final IllegalStateException e) {
            aborted = true;
        } finally {
            transaction.close();
        }
        Assertions.checkTrue(aborted, "the aborting source must propagate out of the writer");

        Assertions.checkEquals(frontiers.size(), 2,
                "the boundary commit and the publish at close must both notify: " + frontiers);
        Assertions.assertThat(frontiers.get(0)).isEqualTo(allValues.get(firstBoundaryValues - 1));
        //close() publishes everything the attempt FINISHED after its last boundary, so the second notification is the
        //frontier of what the aborted pass actually stored - a consumer mirroring the store would otherwise keep
        //demanding a range that is already on disk
        Assertions.assertThat(frontiers.get(1)).isEqualTo(allValues.get(landedValues - 1));
        //and the last notified frontier is exactly what the store now reads as its last published value
        Assertions.assertThat(lookup.getLastValueEndTime()).isEqualTo(frontiers.get(1));
    }

    /**
     * A pass that never completes a commit interval still publishes once - at close - and that publish notifies as
     * well: the hook reports the published frontier, and after close() the store reads up to what this pass landed.
     * A transaction that stages nothing publishes nothing and notifies nothing.
     */
    @Test
    public void testTheClosePublishNotifiesAndAnEmptyAttemptDoesNot() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("hookQuiet");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final int commitInterval = TimeSeriesUpdateTransaction.PUBLISH_SEGMENTS_PER_GENERATION;
        final List<FDate> frontiers = new ArrayList<>();
        final TimeSeriesUpdateTransaction<FDate> transaction = lookup.newUpdateTransaction(true);
        transaction.setBoundaryCommitListener(new ITimeSeriesBoundaryCommitListener() {

            @Override
            public void onBoundaryCommitPublished(final FDate publishedEndTime) {
                frontiers.add(publishedEndTime);
            }
        });
        //a pass that never completes a full commit interval of segments never publishes a boundary: no notification
        final List<FDate> allValues = newValues(1, (commitInterval - 1) * BATCH_FLUSH_INTERVAL);
        try {
            SequentialChunkedUpdateProgress.doUpdate(transaction,
                    new UpdatingInternalMethods(table, lookup, transaction.getEpochLocalDataId()),
                    transaction.getPrecedingMemoryOffset(), transaction.getMemoryOffset(),
                    transaction.getPrecedingValueCount(), WrapperCloseableIterable.maybeWrap(allValues));
            transaction.commit();
        } finally {
            transaction.close();
        }
        Assertions.checkEquals(frontiers.size(), 1,
                "a pass without a boundary commit publishes at close and must notify that frontier: " + frontiers);
        Assertions.assertThat(frontiers.get(0)).isEqualTo(allValues.get(allValues.size() - 1));
        Assertions.assertThat(lookup.getLastValueEndTime()).isEqualTo(frontiers.get(0));

        //the other half of the rule: nothing published means nothing reported
        final List<FDate> emptyAttemptFrontiers = new ArrayList<>();
        final TimeSeriesUpdateTransaction<FDate> emptyAttempt = lookup.newUpdateTransaction(true);
        emptyAttempt.setBoundaryCommitListener(new ITimeSeriesBoundaryCommitListener() {

            @Override
            public void onBoundaryCommitPublished(final FDate publishedEndTime) {
                emptyAttemptFrontiers.add(publishedEndTime);
            }
        });
        emptyAttempt.close();
        Assertions.checkTrue(emptyAttemptFrontiers.isEmpty(),
                "an attempt that staged nothing must not notify: " + emptyAttemptFrontiers);
    }

    @Test
    public void testUpdaterAttemptNotifiesListenerFromItsOwnHook() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("hookUpdater");
        final int commitInterval = TimeSeriesUpdateTransaction.PUBLISH_SEGMENTS_PER_GENERATION;
        final int firstBoundaryValues = commitInterval * BATCH_FLUSH_INTERVAL;
        final List<FDate> allValues = newValues(1, 2 * firstBoundaryValues);
        final List<FDate> frontiers = new ArrayList<>();
        final ICloseableIterable<? extends FDate> source = abortingSource(allValues,
                firstBoundaryValues + BATCH_FLUSH_INTERVAL);
        final ATimeSeriesUpdater<String, FDate> updater = new ATimeSeriesUpdater<String, FDate>(KEY, table) {

            @Override
            protected ICloseableIterable<? extends FDate> getSource(final FDate updateFrom) {
                return source;
            }

            @Override
            protected boolean shouldWriteInParallel() {
                return false;
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

            @Override
            protected ITimeSeriesBoundaryCommitListener newBoundaryCommitListener() {
                return new ITimeSeriesBoundaryCommitListener() {

                    @Override
                    public void onBoundaryCommitPublished(final FDate publishedEndTime) {
                        frontiers.add(publishedEndTime);
                    }
                };
            }
        };
        boolean aborted = false;
        try {
            updater.update().close();
        } catch (final IncompleteUpdateRetryableException e) {
            aborted = true;
        }
        Assertions.checkTrue(aborted, "the aborted pass must propagate");
        Assertions.checkTrue(!frontiers.isEmpty(), "the attempt's boundary commit must notify its listener");
        Assertions.assertThat(frontiers.get(0)).isEqualTo(allValues.get(firstBoundaryValues - 1));
        for (int i = 1; i < frontiers.size(); i++) {
            Assertions.checkTrue(frontiers.get(i).isAfterNotNullSafe(frontiers.get(i - 1)),
                    "the notified frontiers must advance: " + frontiers);
        }
        //the published frontier the listener saw is the store's readable high-water after the abort
        Assertions.assertThat(table.getLookupTableCache(KEY).getLastValueEndTime())
                .isEqualTo(frontiers.get(frontiers.size() - 1));
    }

    /**
     * A source that hands out {@code landed} values and then throws - the stream-death shape of
     * {@link TimeSeriesResumableUpdateAttemptTest}.
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
                            throw new IllegalStateException("abort mid-pass after " + landed + " fetched values");
                        }
                        return values.get(index++);
                    }

                    @Override
                    public void close() {}
                };
            }
        };
    }

    private static List<FDate> newValues(final int fromDay, final int toDay) {
        final List<FDate> values = new ArrayList<>();
        for (int day = fromDay; day <= toDay; day++) {
            values.add(FDateBuilder.newDate(2020, 1, 1).addDays(day));
        }
        return values;
    }

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
            return true;
        }

        @Override
        public String getEpochLocalDataId() {
            return epochLocalDataId;
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

}
