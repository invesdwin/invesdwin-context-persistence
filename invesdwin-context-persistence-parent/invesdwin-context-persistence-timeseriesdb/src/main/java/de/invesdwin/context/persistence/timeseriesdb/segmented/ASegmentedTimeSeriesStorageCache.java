package de.invesdwin.context.persistence.timeseriesdb.segmented;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang3.SerializationException;
import org.springframework.retry.backoff.BackOffPolicy;

import de.invesdwin.context.integration.DatabaseThreads;
import de.invesdwin.context.integration.retry.NonBlockingRetryLaterRuntimeException;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.integration.retry.task.ARetryCallable;
import de.invesdwin.context.integration.retry.task.BackOffPolicies;
import de.invesdwin.context.integration.retry.task.RetryOriginator;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.persistence.ezdb.table.range.ADelegateRangeTable;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupMode;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.buffer.FileBufferCache;
import de.invesdwin.context.persistence.timeseriesdb.loop.AShiftBackUnitsLoopLongIndex;
import de.invesdwin.context.persistence.timeseriesdb.loop.AShiftForwardUnitsLoopLongIndex;
import de.invesdwin.context.persistence.timeseriesdb.loop.ShiftBackUnitsLoop;
import de.invesdwin.context.persistence.timeseriesdb.loop.ShiftForwardUnitsLoop;
import de.invesdwin.context.persistence.timeseriesdb.segmented.finder.ISegmentFinder;
import de.invesdwin.context.persistence.timeseriesdb.storage.ISkipFileFunction;
import de.invesdwin.context.persistence.timeseriesdb.storage.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.SingleValue;
import de.invesdwin.context.persistence.timeseriesdb.storage.cache.ALatestValueByIndexCache;
import de.invesdwin.context.persistence.timeseriesdb.storage.key.RangeShiftUnitsKey;
import de.invesdwin.context.persistence.timeseriesdb.updater.ALoggingTimeSeriesUpdater;
import de.invesdwin.context.persistence.timeseriesdb.updater.ITimeSeriesUpdater;
import de.invesdwin.util.collections.eviction.EvictionMode;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.iterable.ATransformingIterable;
import de.invesdwin.util.collections.iterable.ATransformingIterator;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterable;
import de.invesdwin.util.collections.iterable.FlatteningIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.skip.ASkippingIterable;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import de.invesdwin.util.collections.loadingcache.ILoadingCache;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.Threads;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.Locks;
import de.invesdwin.util.concurrent.lock.disabled.DisabledLock;
import de.invesdwin.util.concurrent.lock.readwrite.IReadWriteLock;
import de.invesdwin.util.concurrent.reference.WeakThreadLocalReference;
import de.invesdwin.util.concurrent.taskinfo.provider.TaskInfoCallable;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.range.TimeRange;
import ezdb.table.RangeTableRow;

@NotThreadSafe
public abstract class ASegmentedTimeSeriesStorageCache<K, V> implements Closeable {
    public static final Integer MAXIMUM_SIZE = TimeSeriesStorageCache.MAXIMUM_SIZE;
    public static final EvictionMode EVICTION_MODE = TimeSeriesStorageCache.EVICTION_MODE;
    public static final boolean HIGH_CONCURRENCY = TimeSeriesStorageCache.HIGH_CONCURRENCY;

    private static final WrappedExecutorService LOAD_INDEX_EXECUTOR;
    private static final WrappedExecutorService MAYBE_INIT_SEGMENT_ASYNC_EXECUTOR;

    static {
        LOAD_INDEX_EXECUTOR = Executors
                .newFixedThreadPool(ASegmentedTimeSeriesStorageCache.class.getSimpleName() + "_LOAD_INDEX", 1);
        MAYBE_INIT_SEGMENT_ASYNC_EXECUTOR = Executors.newFixedThreadPool(
                ASegmentedTimeSeriesStorageCache.class.getSimpleName() + "_MAYBE_INIT_SEGMENT_ASYNC_EXECUTOR",
                Executors.getCpuThreadPoolCount());
    }

    private volatile boolean closed;
    private volatile Optional<V> cachedFirstValue;
    private volatile Optional<V> cachedLastValue;
    private volatile long cachedSize = -1L;
    private volatile Optional<FDate> cachedPrevLastAvailableSegmentToWithoutLive;
    private volatile Optional<FDate> cachedPrevLastAvailableSegmentToWithLive;
    private final Log log = new Log(this);

    private final ASegmentedTimeSeriesDB<K, V>.SegmentedTable segmentedTable;
    private final TimeSeriesLookupMode lookupMode;
    private final SegmentedTimeSeriesStorage storage;
    private final K key;
    private final String hashKey;
    private final ISerde<V> valueSerde;
    private final Function<SegmentedKey<K>, ICloseableIterable<? extends V>> source;
    private final Map<SegmentedKey<K>, Long> precedingValueCountCache = ILockCollectionFactory.getInstance(true)
            .newConcurrentMap();
    private final ILoadingCache<Long, IndexedSegmentedKey<K>> latestSegmentedKeyFromIndexCache = new ALoadingCache<Long, IndexedSegmentedKey<K>>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return MAXIMUM_SIZE;
        }

        @Override
        protected EvictionMode getEvictionMode() {
            return EVICTION_MODE;
        }

        @Override
        protected boolean isHighConcurrency() {
            return HIGH_CONCURRENCY;
        }

        @Override
        protected IndexedSegmentedKey<K> loadValue(final Long key) {
            return newLatestSegmentedKeyFromIndex(key);
        }

    };
    private final ILoadingCache<FDate, Long> latestValueIndexLookupCache = new ALoadingCache<FDate, Long>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return MAXIMUM_SIZE;
        }

        @Override
        protected EvictionMode getEvictionMode() {
            return EVICTION_MODE;
        }

        @Override
        protected boolean isHighConcurrency() {
            return HIGH_CONCURRENCY;
        }

        @Override
        protected Long loadValue(final FDate key) {
            return latestValueIndexLookup(key);
        }
    };
    private final ILoadingCache<RangeShiftUnitsKey, Long> previousValueIndexLookupCache = new ALoadingCache<RangeShiftUnitsKey, Long>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return MAXIMUM_SIZE;
        }

        @Override
        protected EvictionMode getEvictionMode() {
            return EVICTION_MODE;
        }

        @Override
        protected boolean isHighConcurrency() {
            return HIGH_CONCURRENCY;
        }

        @Override
        protected Long loadValue(final RangeShiftUnitsKey key) {
            return previousValueIndexLookup(key.getRangeKey(), key.getShiftUnits());
        }
    };
    private final ILoadingCache<RangeShiftUnitsKey, Long> nextValueIndexLookupCache = new ALoadingCache<RangeShiftUnitsKey, Long>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return MAXIMUM_SIZE;
        }

        @Override
        protected EvictionMode getEvictionMode() {
            return EVICTION_MODE;
        }

        @Override
        protected boolean isHighConcurrency() {
            return HIGH_CONCURRENCY;
        }

        @Override
        protected Long loadValue(final RangeShiftUnitsKey key) {
            return nextValueIndexLookup(key.getRangeKey(), key.getShiftUnits());
        }

    };
    private final WeakThreadLocalReference<ALatestValueByIndexCache<V>> latestValueByIndexCacheHolder = new WeakThreadLocalReference<ALatestValueByIndexCache<V>>() {
        @Override
        protected ALatestValueByIndexCache<V> initialValue() {
            return new LatestValueByIndexCache();
        };
    };
    @GuardedBy("precedingValueCountCache")
    private boolean lookupByIndexAvailable;
    @GuardedBy("precedingValueCountCache")
    private Future<?> lookupByIndexAvailableFuture;
    @GuardedBy("precedingValueCountCache")
    private long lookupByIndexAvailableFutureLastRunNanos = Instant.DUMMY_NANOS;
    private volatile boolean lookupByIndexAvailableFutureDisabled;
    private final ILock deleteLock;
    private final Map<SegmentedKey<K>, Future<?>> segmentedKey_maybeInitSegmentAsyncFuture = ILockCollectionFactory
            .getInstance(true)
            .newConcurrentMap();
    private volatile int lastResetIndex = 0;

    public ASegmentedTimeSeriesStorageCache(final ASegmentedTimeSeriesDB<K, V>.SegmentedTable segmentedTable,
            final SegmentedTimeSeriesStorage storage, final K key, final String hashKey) {
        this.storage = storage;
        this.segmentedTable = segmentedTable;
        this.lookupMode = segmentedTable.getLookupMode();
        this.key = key;
        this.hashKey = hashKey;
        this.valueSerde = segmentedTable.getValueSerde();
        this.source = new Function<SegmentedKey<K>, ICloseableIterable<? extends V>>() {
            @Override
            public ICloseableIterable<? extends V> apply(final SegmentedKey<K> t) {
                return downloadSegmentElements(t);
            }
        };
        this.deleteLock = Locks.newReentrantLock(ASegmentedTimeSeriesStorageCache.class.getSimpleName() + "_"
                + segmentedTable.getName() + "_" + hashKey + "_deleteLock");
    }

    public ICloseableIterable<V> readRangeValues(final FDate from, final FDate to, final ILock readLock,
            final ISkipFileFunction skipFileFunction) {
        final FDate firstAvailableSegmentFrom = getFirstAvailableSegmentFrom(key);
        if (firstAvailableSegmentFrom == null) {
            return EmptyCloseableIterable.getInstance();
        }
        final FDate lastAvailableSegmentTo = getLastAvailableSegmentTo(key, to);
        if (lastAvailableSegmentTo == null) {
            return EmptyCloseableIterable.getInstance();
        }
        //adjust dates directly to prevent unnecessary segment calculations
        final FDate adjFrom = FDates.max(from, firstAvailableSegmentFrom);
        final FDate adjTo = FDates.min(to, lastAvailableSegmentTo);
        final ISegmentFinder segmentFinder = getSegmentFinder(key);
        final ICloseableIterable<TimeRange> segments = getSegments(segmentFinder, segmentFinder.getDay(adjFrom),
                segmentFinder.getDay(adjTo), lastAvailableSegmentTo);
        final ATransformingIterable<TimeRange, ICloseableIterable<V>> segmentQueries = new ATransformingIterable<TimeRange, ICloseableIterable<V>>(
                segments) {
            @Override
            protected ICloseableIterable<V> transform(final TimeRange value) {
                return new ICloseableIterable<V>() {
                    @Override
                    public ICloseableIterator<V> iterator() {
                        final SegmentedKey<K> segmentedKey = new SegmentedKey<K>(key, value);
                        maybeInitSegment(segmentedKey);
                        final FDate segmentAdjFrom = FDates.max(adjFrom, value.getFrom());
                        final FDate segmentAdjTo = FDates.min(adjTo, value.getTo());
                        final ILock compositeReadLock = Locks.newCompositeLock(readLock,
                                segmentedTable.getTableLock(segmentedKey).readLock());
                        return segmentedTable.getLookupTableCache(segmentedKey)
                                .readRangeValues(segmentAdjFrom, segmentAdjTo, compositeReadLock, skipFileFunction);
                    }
                };
            }
        };
        final ICloseableIterable<V> rangeValues = new FlatteningIterable<V>(segmentQueries);
        return rangeValues;
    }

    private ICloseableIterable<TimeRange> getSegments(final ISegmentFinder segmentFinder, final FDate from,
            final FDate to, final FDate lastAvailableSegmentTo) {
        if (from == null || to == null) {
            return EmptyCloseableIterable.getInstance();
        }
        final TimeRange nextSegment = segmentFinder.getCacheQuery().getValue(to.addMilliseconds(1));
        final FDate adjTo;
        if (to.equalsNotNullSafe(lastAvailableSegmentTo) && nextSegment.getFrom().equalsNotNullSafe(to)) {
            //adjust for overlapping segments
            adjTo = to.addMilliseconds(-1);
        } else {
            adjTo = to;
        }
        final FDate adjFrom = from;
        final ICloseableIterable<TimeRange> segments = new ICloseableIterable<TimeRange>() {
            @Override
            public ICloseableIterator<TimeRange> iterator() {
                return new ICloseableIterator<TimeRange>() {

                    private TimeRange nextSegment = segmentFinder.getCacheQuery().getValue(adjFrom);

                    @Override
                    public boolean hasNext() {
                        return nextSegment != null && nextSegment.getFrom().isBeforeOrEqualTo(adjTo);
                    }

                    @Override
                    public TimeRange next() {
                        if (nextSegment == null) {
                            throw FastNoSuchElementException
                                    .getInstance("ASegmentedTimeSeriesStorageCache getSegments nextSegment is null");
                        }
                        final TimeRange curSegment = nextSegment;
                        //get one segment later
                        nextSegment = determineNextSegment(curSegment);
                        return curSegment;
                    }

                    private TimeRange determineNextSegment(final TimeRange curSegment) {
                        final FDate nextSegmentStart = nextSegment.getTo().addMilliseconds(1);
                        final TimeRange nextSegment = segmentFinder.getCacheQueryWithFutureNull()
                                .getValue(nextSegmentStart);
                        if (!curSegment.getTo().equalsNotNullSafe(nextSegment.getFrom())
                                && !nextSegmentStart.equalsNotNullSafe(nextSegment.getFrom())) {
                            //allow overlapping segments
                            throw new IllegalStateException("Segment start expected [" + curSegment.getTo() + " or "
                                    + nextSegmentStart + "] != found [" + nextSegment.getFrom() + "]");
                        }
                        return nextSegment;
                    }

                    @Override
                    public void close() {
                        nextSegment = null;
                    }
                };
            }
        };
        final ASkippingIterable<TimeRange> filteredSegments = new ASkippingIterable<TimeRange>(segments) {
            @Override
            protected boolean skip(final TimeRange element) {
                //though additionally skip ranges that exceed the available dates
                final FDate segmentTo = element.getTo();
                if (segmentTo.isBefore(adjFrom)) {
                    throw new IllegalStateException(
                            "segmentTo [" + segmentTo + "] should not be before adjFrom [" + adjFrom + "]");
                }
                final FDate segmentFrom = element.getFrom();
                if (segmentFrom.isAfter(adjTo)) {
                    //no need to continue going higher
                    throw FastNoSuchElementException
                            .getInstance("ASegmentedTimeSeriesStorageCache getSegments end reached");
                }
                return false;
            }
        };
        return filteredSegments;
    }

    protected abstract ISegmentFinder getSegmentFinder(K key);

    public void maybeInitSegment(final SegmentedKey<K> segmentedKey) {
        if (DatabaseThreads.isThreadBlockingUpdateDatabaseDisabled()) {
            maybeInitSegmentAsync(segmentedKey);
        } else {
            maybeInitSegmentSync(segmentedKey, source);
        }
    }

    private boolean maybeInitSegmentAsync(final SegmentedKey<K> segmentedKey) {
        if (!assertValidSegment(segmentedKey)) {
            return false;
        }
        //1. check segment status in series storage
        final IReadWriteLock segmentTableLock = segmentedTable.getTableLock(segmentedKey);
        final ILock segmentReadLock = segmentTableLock.readLock();
        if (!segmentReadLock.tryLockNoInterrupt(TimeSeriesProperties.NON_BLOCKING_ASYNC_UPDATE_WAIT_TIMEOUT)) {
            throw new NonBlockingRetryLaterRuntimeException(ASegmentedTimeSeriesStorageCache.class.getSimpleName()
                    + ".maybeInitSegmentAsync: readlock could not be acquired for async update check while operating in non-blocking mode for segment "
                    + getElementsName() + ": " + segmentedKey);
        }
        final SegmentStatus status;
        try {
            status = storage.getSegmentStatusTable().get(hashKey, segmentedKey.getSegment());
        } finally {
            segmentReadLock.unlock();
        }
        //2. if not existing or false, set status to false -> start segment update -> after update set status to true
        if (status == null || status == SegmentStatus.INITIALIZING) {
            Future<?> future = segmentedKey_maybeInitSegmentAsyncFuture.get(segmentedKey);
            final String reason;
            if (future == null || future.isDone()) {
                if (Threads.isInterrupted()) {
                    //abort when shutting down
                    return false;
                }
                //we can trigger or retry an async update
                future = MAYBE_INIT_SEGMENT_ASYNC_EXECUTOR.submit(() -> {
                    try {
                        if (Threads.isInterrupted()) {
                            //abort when shutting down
                            return;
                        }
                        maybeInitSegmentSync(segmentedKey, source);
                    } catch (final Throwable t) {
                        throw Err.process(t);
                    } finally {
                        segmentedKey_maybeInitSegmentAsyncFuture.remove(segmentedKey);
                    }
                });
                segmentedKey_maybeInitSegmentAsyncFuture.put(segmentedKey, future);
                if (future.isDone()) {
                    //make sure entry is removed even if async task finished before the put operation happened
                    segmentedKey_maybeInitSegmentAsyncFuture.remove(segmentedKey);
                }
                reason = "started";
            } else {
                reason = "is in progress";
            }

            try {
                Futures.waitNoInterrupt(future, TimeSeriesProperties.NON_BLOCKING_ASYNC_UPDATE_WAIT_TIMEOUT);
                //make sure entry is removed even if async task finished before the put operation happened
                segmentedKey_maybeInitSegmentAsyncFuture.remove(segmentedKey);
            } catch (final TimeoutException e) {
                throw new NonBlockingRetryLaterRuntimeException(
                        ASegmentedTimeSeriesStorageCache.class.getSimpleName() + ".maybeInitSegmentAsync: async update "
                                + reason + " while operating in non-blocking mode for segment " + getElementsName()
                                + ": " + segmentedKey);
            }
        }
        //3. if true do nothing
        return false;
    }

    public boolean maybeInitSegmentSync(final SegmentedKey<K> segmentedKey,
            final Function<SegmentedKey<K>, ICloseableIterable<? extends V>> source) {
        if (!assertValidSegment(segmentedKey)) {
            return false;
        }
        //1. check segment status in series storage
        final IReadWriteLock segmentTableLock = segmentedTable.getTableLock(segmentedKey);
        /*
         * We need this synchronized block so that we don't collide on the write lock not being possible to be acquired
         * after 1 minute. The ReadWriteLock object should be safe to lock via synchronized keyword since no internal
         * synchronization occurs on that object itself
         */
        synchronized (segmentTableLock) {
            final SegmentStatus status = getSegmentStatusWithReadLock(segmentedKey, segmentTableLock);
            //2. if not existing or false, set status to false -> start segment update -> after update set status to true
            if (status == null || status == SegmentStatus.INITIALIZING) {
                final ILock segmentWriteLock = segmentTableLock.writeLock();
                try {
                    if (!segmentWriteLock.tryLock(TimeSeriesProperties.ACQUIRE_WRITE_LOCK_TIMEOUT)) {
                        /*
                         * should not happen here because segment should not yet exist. Though if it happens we would
                         * rather like an exception instead of a deadlock!
                         */
                        throw Locks.getLockTrace()
                                .handleLockException(segmentWriteLock.getName(),
                                        new RetryLaterRuntimeException("Write lock could not be acquired for table ["
                                                + segmentedTable.getName() + "] and key [" + segmentedKey
                                                + "]. Please ensure all iterators are closed!"));
                    }
                } catch (final InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
                try {
                    // no double checked locking required between read and write lock here because of the outer synchronized block
                    if (status == SegmentStatus.INITIALIZING) {
                        //initialization got aborted, retry from a fresh state
                        segmentedTable.deleteRange(segmentedKey);
                        storage.getSegmentStatusTable().delete(hashKey, segmentedKey.getSegment());
                    }
                    initSegmentWithStatusHandling(segmentedKey, source);
                    onSegmentCompleted(segmentedKey, readRangeValues(segmentedKey.getSegment().getFrom(),
                            segmentedKey.getSegment().getTo(), DisabledLock.INSTANCE, null));
                    return true;
                } finally {
                    segmentWriteLock.unlock();
                }
            }
        }
        //3. if true do nothing
        return false;
    }

    private boolean assertValidSegment(final SegmentedKey<K> segmentedKey) {
        final FDate firstAvailableSegmentFrom = getFirstAvailableSegmentFrom(segmentedKey.getKey());
        if (firstAvailableSegmentFrom == null) {
            return false;
        }
        final FDate lastAvailableSegmentTo = getLastAvailableSegmentTo(segmentedKey.getKey(),
                segmentedKey.getSegment().getTo());
        if (lastAvailableSegmentTo == null) {
            return false;
        }
        if (firstAvailableSegmentFrom.isAfterNotNullSafe(lastAvailableSegmentTo)) {
            throw new IllegalStateException(segmentedKey + ": firstAvailableSegmentFrom [" + firstAvailableSegmentFrom
                    + "] should not be after lastAvailableSegmentTo [" + lastAvailableSegmentTo + "]");
        }
        //throw error if a segment is being updated that is beyond the lastAvailableSegmentTo
        final FDate segmentFrom = segmentedKey.getSegment().getTo();
        if (segmentFrom.isBefore(firstAvailableSegmentFrom)) {
            throw new IllegalStateException(segmentedKey + ": segmentFrom [" + segmentFrom
                    + "] should not be before firstAvailableSegmentFrom [" + firstAvailableSegmentFrom + "]");
        }
        final FDate segmentTo = segmentedKey.getSegment().getTo();
        if (segmentTo.isAfterNotNullSafe(lastAvailableSegmentTo)) {
            //            throw new IllegalStateException(segmentedKey + ": segmentTo [" + segmentTo
            //                    + "] should not be after lastAvailableSegmentTo [" + lastAvailableSegmentTo + "]");
            //might happen very rarely when segment to is still initializing, for now just returning false to skip init
            //            Caused by: java.lang.IllegalStateException: SegmentedKey[key:FXCM:EURUSD|segment:2020-08-01T00:00:00.000 -> 2020-08-31T23:59:59.999 => P1MT23H59M59.999S]: segmentTo [2020-08-31T23:59:59.999] should not be after lastAvailableSegmentTo [2020-07-31T23:59:59.999]
            //                    at de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.ASegmentedTimeSeriesStorageCache.assertValidSegment(ASegmentedTimeSeriesStorageCache.java:454)
            //                    at de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.ASegmentedTimeSeriesStorageCache.maybeInitSegment(ASegmentedTimeSeriesStorageCache.java:382)
            //                    at de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.ASegmentedTimeSeriesStorageCache.maybeInitSegment(ASegmentedTimeSeriesStorageCache.java:377)
            //                    at de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.ASegmentedTimeSeriesStorageCache$1$1.apply(ASegmentedTimeSeriesStorageCache.java:101)
            //                    at de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.ASegmentedTimeSeriesStorageCache$1$1.apply(ASegmentedTimeSeriesStorageCache.java:1)
            //                    at de.invesdwin.context.persistence.timeseries.ezdb.ADelegateRangeTable.getOrLoad(ADelegateRangeTable.java:481)
            //                    at de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.ASegmentedTimeSeriesStorageCache$1.loadValue(ASegmentedTimeSeriesStorageCache.java:85)
            //                    at de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.ASegmentedTimeSeriesStorageCache$1.loadValue(ASegmentedTimeSeriesStorageCache.java:1)
            //                    at de.invesdwin.util.collections.loadingcache.ALoadingCache$1.apply(ALoadingCache.java:54)
            //                    at de.invesdwin.util.collections.loadingcache.map.ASynchronizedMapLoadingCache.get(ASynchronizedMapLoadingCache.java:38)
            //                    at de.invesdwin.util.collections.loadingcache.ADelegateLoadingCache.get(ADelegateLoadingCache.java:29)
            //                    at de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.ASegmentedTimeSeriesStorageCache.getLatestValue(ASegmentedTimeSeriesStorageCache.java:772)
            //                    at de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.ASegmentedTimeSeriesDB.getLatestValue(ASegmentedTimeSeriesDB.java:265)
            //                    at de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.live.LiveSegmentedTimeSeriesStorageCache$2.apply(LiveSegmentedTimeSeriesStorageCache.java:43)
            //                    at de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.live.LiveSegmentedTimeSeriesStorageCache$2.apply(LiveSegmentedTimeSeriesStorageCache.java:1)
            //                    at de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.live.LiveSegmentedTimeSeriesStorageCache.getLatestValue(LiveSegmentedTimeSeriesStorageCache.java:171)
            //                    at de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.live.ALiveSegmentedTimeSeriesDB.getLatestValue(ALiveSegmentedTimeSeriesDB.java:326)
            //                    at de.invesdwin.trading.financialdata.live.bars.calculated.internal.LiveCalculatedTickCache.getLatestTick(LiveCalculatedTickCache.java:210)
            //                    at de.invesdwin.trading.financialdata.live.bars.FinancialdataLiveTickCache$DelegateTickCache.readLatestValueFor(FinancialdataLiveTickCache.java:194)
            //                    at de.invesdwin.trading.financialdata.live.bars.FinancialdataLiveTickCache$DelegateTickCache.readLatestValueFor(FinancialdataLiveTickCache.java:1)
            //                    at de.invesdwin.util.collections.loadingcache.historical.AGapHistoricalCache.readNewestValueFromDB(AGapHistoricalCache.java:523)
            //                    at de.invesdwin.util.collections.loadingcache.historical.AGapHistoricalCache.loadValue(AGapHistoricalCache.java:152)
            //                    at de.invesdwin.util.collections.loadingcache.historical.AHistoricalCache$InnerLoadingCache$1.apply(AHistoricalCache.java:465)
            //                    ... 52 common frames omitted
            return false;
        }
        return true;
    }

    private void initSegmentWithStatusHandling(final SegmentedKey<K> segmentedKey,
            final Function<SegmentedKey<K>, ICloseableIterable<? extends V>> source) {
        storage.getSegmentStatusTable().put(hashKey, segmentedKey.getSegment(), SegmentStatus.INITIALIZING);
        maybePrepareForUpdate(segmentedKey.getSegment());
        initSegmentRetry(segmentedKey, source);
        if (segmentedTable.isEmptyOrInconsistent(segmentedKey)) {
            storage.getSegmentStatusTable().put(hashKey, segmentedKey.getSegment(), SegmentStatus.COMPLETE_EMPTY);
        } else {
            storage.getSegmentStatusTable().put(hashKey, segmentedKey.getSegment(), SegmentStatus.COMPLETE);
        }
    }

    private SegmentStatus getSegmentStatusWithReadLock(final SegmentedKey<K> segmentedKey,
            final IReadWriteLock segmentTableLock) {
        final ILock segmentReadLock = segmentTableLock.readLock();
        segmentReadLock.lock();
        try {
            return storage.getSegmentStatusTable().get(hashKey, segmentedKey.getSegment());
        } finally {
            segmentReadLock.unlock();
        }
    }

    private void initSegmentRetry(final SegmentedKey<K> segmentedKey,
            final Function<SegmentedKey<K>, ICloseableIterable<? extends V>> source) {
        final ARetryCallable<Throwable> retryTask = new ARetryCallable<Throwable>(
                new RetryOriginator(ASegmentedTimeSeriesDB.class, "initSegment", segmentedKey)) {
            @Override
            protected Throwable callRetry() throws Exception {
                try {
                    if (closed) {
                        return new RetryLaterRuntimeException(ASegmentedTimeSeriesStorageCache.class.getSimpleName()
                                + " for [" + hashKey + "] is already closed.");
                    } else {
                        initSegment(segmentedKey, source);
                    }
                    return null;
                } catch (final Throwable t) {
                    if (closed) {
                        return t;
                    } else {
                        throw t;
                    }
                }
            }

            @Override
            protected BackOffPolicy getBackOffPolicyOverride() {
                //randomize backoff to prevent race conditions between multiple processes
                return BackOffPolicies.randomFixedBackOff(Duration.ONE_SECOND);
            }
        };
        final Throwable t = retryTask.call();
        if (t != null) {
            throw Throwables.propagate(t);
        }
    }

    private void initSegment(final SegmentedKey<K> segmentedKey,
            final Function<SegmentedKey<K>, ICloseableIterable<? extends V>> source) {
        try {
            final ITimeSeriesUpdater<SegmentedKey<K>, V> updater = newSegmentUpdater(segmentedKey, source);
            final Callable<Void> task = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    //write lock is reentrant
                    updater.update();
                    return null;
                }
            };
            final String taskName = "Loading " + getElementsName() + " for " + hashKey;
            final Callable<Percent> progress = new Callable<Percent>() {
                @Override
                public Percent call() throws Exception {
                    return updater.getProgress();
                }
            };
            TaskInfoCallable.of(taskName, task, progress).call();
            final FDate minTime = updater.getMinTime();
            if (minTime != null) {
                final FDate segmentFrom = segmentedKey.getSegment().getFrom();
                final TimeRange prevSegment = getSegmentFinder(segmentedKey.getKey()).getCacheQuery()
                        .getValue(segmentFrom.addMilliseconds(-1));
                if (prevSegment.getTo().equalsNotNullSafe(segmentFrom) && minTime.isBeforeOrEqualTo(segmentFrom)) {
                    throw new IllegalStateException(
                            segmentedKey + ": minTime [" + minTime + "] should not be before or equal to segmentFrom ["
                                    + segmentFrom + "] when overlapping segments are used");
                } else if (minTime.isBefore(segmentFrom)) {
                    throw new IllegalStateException(
                            segmentedKey + ": minTime [" + minTime + "] should not be before segmentFrom ["
                                    + segmentFrom + "] when non overlapping segments are used");
                }
                final FDate maxTime = updater.getMaxTime();
                final FDate segmentTo = segmentedKey.getSegment().getTo();
                if (maxTime.isAfter(segmentTo)) {
                    throw new IllegalStateException(segmentedKey + ": maxTime [" + maxTime
                            + "] should not be after segmentTo [" + segmentTo + "]");
                }
            }
        } catch (final Throwable t) {
            if (Throwables.isCausedByType(t, IncompleteUpdateRetryableException.class)) {
                segmentedTable.deleteRange(new SegmentedKey<K>(segmentedKey.getKey(), segmentedKey.getSegment()));
                throw new RetryLaterRuntimeException(t);
            } else {
                throw Throwables.propagate(t);
            }
        }
    }

    private ITimeSeriesUpdater<SegmentedKey<K>, V> newSegmentUpdater(final SegmentedKey<K> segmentedKey,
            final Function<SegmentedKey<K>, ICloseableIterable<? extends V>> source) {
        ITimeSeriesUpdater<SegmentedKey<K>, V> updater = newSegmentUpdaterOverride(segmentedKey, segmentedTable,
                source);
        if (updater == null) {
            updater = new ALoggingTimeSeriesUpdater<SegmentedKey<K>, V>(segmentedKey, segmentedTable, log) {

                @Override
                protected ICloseableIterable<? extends V> getSource(final FDate updateFrom) {
                    if (updateFrom != null) {
                        throw new IllegalArgumentException("updateFrom should be null");
                    }
                    return source.apply(segmentedKey);
                }

                @Override
                protected FDate extractStartTime(final V element) {
                    return segmentedTable.extractStartTime(element);
                }

                @Override
                protected FDate extractEndTime(final V element) {
                    return segmentedTable.extractEndTime(element);
                }

                @Override
                protected String keyToString(final SegmentedKey<K> key) {
                    return segmentedTable.hashKeyToString(key);
                }

                @Override
                protected String getElementsName() {
                    return "segment " + ASegmentedTimeSeriesStorageCache.this.getElementsName();
                }

                @Override
                public Percent getProgress(final FDate minTime, final FDate maxTime) {
                    if (minTime == null) {
                        return null;
                    }
                    if (maxTime == null) {
                        return null;
                    }
                    final FDate estimatedTo = segmentedKey.getSegment().getTo();
                    return new Percent(new Duration(minTime, maxTime), new Duration(minTime, estimatedTo))
                            .orLower(Percent.ONE_HUNDRED_PERCENT);
                }
            };
        }
        return updater;
    }

    public void onSegmentCompleted(final SegmentedKey<K> segmentedKey, final ICloseableIterable<V> segmentValues) {
        cachedSize = -1L;
        precedingValueCountCache.clear();
        latestSegmentedKeyFromIndexCache.clear();
        latestValueIndexLookupCache.clear();
        nextValueIndexLookupCache.clear();
        previousValueIndexLookupCache.clear();
        lastResetIndex++;
    }

    protected abstract ITimeSeriesUpdater<SegmentedKey<K>, V> newSegmentUpdaterOverride(SegmentedKey<K> segmentedKey,
            ASegmentedTimeSeriesDB<K, V>.SegmentedTable segmentedTable,
            Function<SegmentedKey<K>, ICloseableIterable<? extends V>> source);

    protected abstract String getElementsName();

    protected abstract ICloseableIterable<? extends V> downloadSegmentElements(SegmentedKey<K> segmentedKey);

    protected abstract FDate getLastAvailableSegmentTo(K key, FDate updateTo);

    protected abstract FDate getFirstAvailableSegmentFrom(K key);

    public ICloseableIterable<V> readRangeValuesReverse(final FDate from, final FDate to, final ILock readLock,
            final ISkipFileFunction skipFileFunction) {
        final FDate firstAvailableSegmentFrom = getFirstAvailableSegmentFrom(key);
        final FDate lastAvailableSegmentTo = getLastAvailableSegmentTo(key, to);
        //adjust dates directly to prevent unnecessary segment calculations
        final FDate adjFrom = FDates.min(from, lastAvailableSegmentTo);
        final FDate adjTo = FDates.max(to, firstAvailableSegmentFrom);
        final ISegmentFinder segmentFinder = getSegmentFinder(key);
        final ICloseableIterable<TimeRange> filteredSegments = getSegmentsReverse(segmentFinder,
                segmentFinder.getDay(adjFrom), segmentFinder.getDay(adjTo), lastAvailableSegmentTo);
        final ATransformingIterable<TimeRange, ICloseableIterable<V>> segmentQueries = new ATransformingIterable<TimeRange, ICloseableIterable<V>>(
                filteredSegments) {
            @Override
            protected ICloseableIterable<V> transform(final TimeRange value) {
                return new ICloseableIterable<V>() {
                    @Override
                    public ICloseableIterator<V> iterator() {
                        final SegmentedKey<K> segmentedKey = new SegmentedKey<K>(key, value);
                        maybeInitSegment(segmentedKey);
                        final FDate segmentAdjFrom = FDates.min(adjFrom, value.getTo());
                        final FDate segmentAdjTo = FDates.max(adjTo, value.getFrom());
                        final ILock compositeReadLock = Locks.newCompositeLock(readLock,
                                segmentedTable.getTableLock(segmentedKey).readLock());
                        return segmentedTable.getLookupTableCache(segmentedKey)
                                .readRangeValuesReverse(segmentAdjFrom, segmentAdjTo, compositeReadLock,
                                        skipFileFunction);
                    }
                };
            }
        };
        final ICloseableIterable<V> rangeValues = new FlatteningIterable<V>(segmentQueries);
        return rangeValues;
    }

    private ICloseableIterable<TimeRange> getSegmentsReverse(final ISegmentFinder segmentFinder, final FDate from,
            final FDate to, final FDate lastAvailableSegmentTo) {
        if (from == null || to == null) {
            return EmptyCloseableIterable.getInstance();
        }
        final TimeRange nextSegment = segmentFinder.getCacheQuery().getValue(from.addMilliseconds(1));
        final FDate adjFrom;
        if (from.equalsNotNullSafe(lastAvailableSegmentTo) && nextSegment.getFrom().equalsNotNullSafe(from)) {
            //adjust for overlapping segments
            adjFrom = from.addMilliseconds(-1);
        } else {
            adjFrom = from;
        }
        final FDate adjTo = to;
        final ICloseableIterable<TimeRange> segments = new ICloseableIterable<TimeRange>() {
            @Override
            public ICloseableIterator<TimeRange> iterator() {
                return new ICloseableIterator<TimeRange>() {

                    private TimeRange nextSegment = segmentFinder.getCacheQuery().getValue(adjFrom);

                    @Override
                    public boolean hasNext() {
                        return nextSegment != null && nextSegment.getTo().isAfter(adjTo);
                    }

                    @Override
                    public TimeRange next() {
                        final TimeRange curSegment = nextSegment;
                        if (curSegment == null) {
                            throw FastNoSuchElementException
                                    .getInstance("ASegmentedTimeSeriesStorageCache getSegments end reached null");
                        }
                        //get one segment earlier
                        nextSegment = segmentFinder.getCacheQueryWithFutureNull()
                                .getValue(nextSegment.getFrom().addMilliseconds(-1));
                        return curSegment;
                    }

                    @Override
                    public void close() {
                        nextSegment = null;
                    }
                };
            }
        };
        final ASkippingIterable<TimeRange> filteredSegments = new ASkippingIterable<TimeRange>(segments) {
            @Override
            protected boolean skip(final TimeRange element) {
                //though additionally skip ranges that exceed the available dates
                final FDate segmentTo = element.getTo();
                if (segmentTo.isBefore(adjTo)) {
                    //no need to continue going lower
                    throw FastNoSuchElementException
                            .getInstance("ASegmentedTimeSeriesStorageCache getSegments end reached adjTo");
                }
                //skip last value and continue with earlier ones
                final FDate segmentFrom = element.getFrom();
                return segmentFrom.isAfter(adjFrom);
            }
        };
        return filteredSegments;
    }

    public void deleteAll() {
        lookupByIndexAvailableFutureDisabled = true;
        try {
            final Future<?> future = lookupByIndexAvailableFuture;
            if (future != null) {
                //abort parallel task to prevent write lock from taking too long to be acquired
                future.cancel(true);
                try {
                    Futures.waitNoInterrupt(future, TimeSeriesProperties.ACQUIRE_WRITE_LOCK_TIMEOUT);
                } catch (final CancellationException e) {
                    //success
                } catch (final TimeoutException e) {
                    throw new RetryLaterRuntimeException("Unable to cancel lookupByIndexAvailableFuture within: "
                            + TimeSeriesProperties.ACQUIRE_WRITE_LOCK_TIMEOUT, e);
                }
            }

            deleteLock.lock();
            try {
                final ADelegateRangeTable<String, TimeRange, SegmentStatus> segmentStatusTable = storage
                        .getSegmentStatusTable();
                final List<TimeRange> rangeKeys;
                try (ICloseableIterator<TimeRange> rangeKeysIterator = new ATransformingIterator<RangeTableRow<String, TimeRange, SegmentStatus>, TimeRange>(
                        segmentStatusTable.range(hashKey)) {

                    @Override
                    protected TimeRange transform(final RangeTableRow<String, TimeRange, SegmentStatus> value) {
                        return value.getRangeKey();
                    }
                }) {
                    rangeKeys = Lists.toListWithoutHasNext(rangeKeysIterator);
                }
                for (int i = 0; i < rangeKeys.size(); i++) {
                    final TimeRange rangeKey = rangeKeys.get(i);
                    segmentedTable.deleteRange(new SegmentedKey<K>(key, rangeKey));
                }
                segmentStatusTable.deleteRange(hashKey);
                storage.deleteRange_latestValueLookupTable(hashKey);
                storage.deleteRange_nextValueLookupTable(hashKey);
                storage.deleteRange_previousValueLookupTable(hashKey);
                clearCaches();
            } finally {
                deleteLock.unlock();
            }
        } finally {
            lookupByIndexAvailableFutureDisabled = false;
        }
    }

    private void clearCaches() {
        FileBufferCache.remove(hashKey);
        cachedFirstValue = null;
        cachedLastValue = null;
        cachedSize = -1L;
        cachedPrevLastAvailableSegmentToWithLive = null;
        cachedPrevLastAvailableSegmentToWithoutLive = null;
        precedingValueCountCache.clear();
        latestSegmentedKeyFromIndexCache.clear();
        latestValueIndexLookupCache.clear();
        nextValueIndexLookupCache.clear();
        previousValueIndexLookupCache.clear();
        lastResetIndex++;
    }

    public long getLatestValueIndex(final FDate pDate) {
        final FDate date = FDates.min(pDate, getLastAvailableSegmentTo(key, pDate));
        final long valueIndex = latestValueIndexLookupCache.get(date);
        return valueIndex;
    }

    private long latestValueIndexLookup(final FDate date) {
        final FDate firstAvailableSegmentFrom = getFirstAvailableSegmentFrom(key);
        //already adjusted on the outside
        final FDate adjFrom = date;
        final FDate adjTo = firstAvailableSegmentFrom;
        final FDate lastAvailableSegmentTo = getLastAvailableSegmentTo(key, adjFrom);
        final ISegmentFinder segmentFinder = getSegmentFinder(key);
        final ICloseableIterable<TimeRange> segmentsReverse = getSegmentsReverse(segmentFinder,
                segmentFinder.getDay(adjFrom), segmentFinder.getDay(adjTo), lastAvailableSegmentTo);
        try (ICloseableIterator<TimeRange> it = segmentsReverse.iterator()) {
            long latestValueIndex = -1L;
            while (it.hasNext()) {
                final TimeRange segment = it.next();
                final SegmentedKey<K> segmentedKey = new SegmentedKey<K>(key, segment);
                maybeInitSegment(segmentedKey);
                final long newValueIndex = segmentedTable.getLatestValueIndex(segmentedKey, date);
                if (newValueIndex != -1L) {
                    final V newValue = segmentedTable.getLatestValue(segmentedKey, newValueIndex);
                    final FDate newValueTime = segmentedTable.extractEndTime(newValue);
                    if (newValueTime.isBeforeOrEqualTo(date)) {
                        /*
                         * even if we got the first value in this segment and it is after the desired key we just
                         * continue to the beginning to search for an earlier value until we reach the overall
                         * firstValue
                         */
                        latestValueIndex = getPrecedingValueCount(segmentedKey) + newValueIndex;
                        break;
                    }
                }
            }
            if (latestValueIndex == -1L && getFirstValue() != null) {
                return 0L;
            }
            if (latestValueIndex == -1L) {
                return -1L;
            }
            return latestValueIndex;
        }
    }

    public V getLatestValue(final FDate date) {
        switch (lookupMode) {
        case Value:
            return getLatestValueByValue(date);
        case ValueUntilIndexAvailable:
            if (isLookupByIndexAvailable()) {
                return getLatestValueByIndex(date);
            } else {
                return getLatestValueByValue(date);
            }
        case Index:
            return getLatestValueByIndex(date);
        default:
            throw UnknownArgumentException.newInstance(TimeSeriesLookupMode.class, lookupMode);
        }
    }

    private V getLatestValueByIndex(final FDate date) {
        final ALatestValueByIndexCache<V> latestValueByIndexCache = latestValueByIndexCacheHolder.get();
        return latestValueByIndexCache.getLatestValueByIndex(date);
    }

    private V getLatestValueByValue(final FDate pDate) {
        final FDate date = FDates.min(pDate, getLastAvailableSegmentTo(key, pDate));
        final SingleValue value = storage.getOrLoad_latestValueLookupTable(hashKey, date, () -> {
            final FDate firstAvailableSegmentFrom = getFirstAvailableSegmentFrom(key);
            //already adjusted on the outside
            final FDate adjFrom = date;
            final FDate adjTo = firstAvailableSegmentFrom;
            final FDate lastAvailableSegmentTo = getLastAvailableSegmentTo(key, adjFrom);
            final ISegmentFinder segmentFinder = getSegmentFinder(key);
            final ICloseableIterable<TimeRange> segmentsReverse = getSegmentsReverse(segmentFinder,
                    segmentFinder.getDay(adjFrom), segmentFinder.getDay(adjTo), lastAvailableSegmentTo);
            try (ICloseableIterator<TimeRange> it = segmentsReverse.iterator()) {
                V latestValue = null;
                while (it.hasNext()) {
                    final TimeRange segment = it.next();
                    final SegmentedKey<K> segmentedKey = new SegmentedKey<K>(key, segment);
                    maybeInitSegment(segmentedKey);
                    final V newValue = segmentedTable.getLatestValue(segmentedKey, date);
                    if (newValue != null) {
                        final FDate newValueTime = segmentedTable.extractEndTime(newValue);
                        if (newValueTime.isBeforeOrEqualTo(date)) {
                            /*
                             * even if we got the first value in this segment and it is after the desired key we just
                             * continue to the beginning to search for an earlier value until we reach the overall
                             * firstValue
                             */
                            latestValue = newValue;
                            break;
                        }
                    }
                }
                if (latestValue == null) {
                    latestValue = getFirstValue();
                }
                if (latestValue == null) {
                    return null;
                }
                return new SingleValue(valueSerde, latestValue);
            }
        });
        if (value == null) {
            return null;
        }
        return value.getValue(valueSerde);
    }

    public V getLatestValue(final long index) {
        final long adjIndex = adjustIndex(index);
        final IndexedSegmentedKey<K> indexedSegmentedKey = getLatestSegmentedKeyFromIndex(adjIndex);
        if (indexedSegmentedKey == null) {
            return null;
        }
        final long segmentedIndex = adjIndex - indexedSegmentedKey.getPrecedingValueCount();
        final V latestValue = segmentedTable.getLatestValue(indexedSegmentedKey.getSegmentedKey(), segmentedIndex);
        return latestValue;
    }

    private long adjustIndex(final long index) {
        final long maxIndex = size() - 1L;
        if (index >= maxIndex) {
            return maxIndex;
        } else if (index <= 0) {
            return 0L;
        } else {
            return index;
        }
    }

    private IndexedSegmentedKey<K> getLatestSegmentedKeyFromIndex(final long index) {
        return latestSegmentedKeyFromIndexCache.get(index);
    }

    private IndexedSegmentedKey<K> newLatestSegmentedKeyFromIndex(final long index) {
        long precedingValueCount = 0;
        try (ICloseableIterator<RangeTableRow<String, TimeRange, SegmentStatus>> rangeValues = storage
                .getSegmentStatusTable()
                .range(hashKey)) {
            while (true) {
                final RangeTableRow<String, TimeRange, SegmentStatus> row = rangeValues.next();
                final SegmentStatus status = row.getValue();
                if (status == SegmentStatus.COMPLETE) {
                    final SegmentedKey<K> segmentedKey = new SegmentedKey<K>(key, row.getRangeKey());
                    final long combinedValueCount = precedingValueCount
                            + segmentedTable.getLookupTableCache(segmentedKey).size();
                    if (combinedValueCount > index) {
                        return new IndexedSegmentedKey<>(segmentedKey, precedingValueCount);
                    } else {
                        precedingValueCount = combinedValueCount;
                    }
                }
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
        return null;
    }

    public V getPreviousValue(final FDate date, final int shiftBackUnits) {
        switch (lookupMode) {
        case Value:
            return getPreviousValueByValue(date, shiftBackUnits);
        case ValueUntilIndexAvailable:
            if (isLookupByIndexAvailable()) {
                return getPreviousValueByIndex(date, shiftBackUnits);
            } else {
                return getPreviousValueByValue(date, shiftBackUnits);
            }
        case Index:
            return getPreviousValueByIndex(date, shiftBackUnits);
        default:
            throw UnknownArgumentException.newInstance(TimeSeriesLookupMode.class, lookupMode);
        }
    }

    private V getPreviousValueByIndex(final FDate date, final int shiftBackUnits) {
        assertShiftUnitsPositiveNonZero(shiftBackUnits);
        final V firstValue = getFirstValue();
        if (firstValue == null) {
            return null;
        }
        final FDate firstTime = segmentedTable.extractEndTime(firstValue);
        if (date.isBeforeOrEqualTo(firstTime)) {
            return firstValue;
        } else {
            final long valueIndex = previousValueIndexLookupCache.get(new RangeShiftUnitsKey(date, shiftBackUnits));
            return getLatestValue(valueIndex);
        }
    }

    private long previousValueIndexLookup(final FDate date, final int shiftBackUnits) {
        final AShiftBackUnitsLoopLongIndex<V> shiftBackLoop = new AShiftBackUnitsLoopLongIndex<V>(date,
                shiftBackUnits) {
            @Override
            protected V getLatestValue(final long index) {
                return ASegmentedTimeSeriesStorageCache.this.getLatestValue(index);
            }

            @Override
            protected long getLatestValueIndex(final FDate date) {
                return ASegmentedTimeSeriesStorageCache.this.getLatestValueIndex(date);
            }

            @Override
            protected FDate extractEndTime(final V value) {
                return segmentedTable.extractEndTime(value);
            }

            @Override
            protected long size() {
                return ASegmentedTimeSeriesStorageCache.this.size();
            }
        };
        shiftBackLoop.loop();
        return shiftBackLoop.getPrevValueIndex();
    }

    private V getPreviousValueByValue(final FDate date, final int shiftBackUnits) {
        assertShiftUnitsPositiveNonZero(shiftBackUnits);
        final V firstValue = getFirstValue();
        final FDate firstTime = segmentedTable.extractEndTime(firstValue);
        if (date.isBeforeOrEqualTo(firstTime)) {
            return firstValue;
        } else {
            final SingleValue value = storage.getOrLoad_previousValueLookupTable(hashKey, date, shiftBackUnits, () -> {
                final ShiftBackUnitsLoop<V> shiftBackLoop = new ShiftBackUnitsLoop<>(date, shiftBackUnits,
                        segmentedTable::extractEndTime);
                final ICloseableIterable<V> rangeValuesReverse = readRangeValuesReverse(date, null,
                        DisabledLock.INSTANCE, new ISkipFileFunction() {
                            @Override
                            public boolean skipFile(final MemoryFileSummary file) {
                                final boolean skip = shiftBackLoop.getPrevValue() != null
                                        && file.getValueCount() < shiftBackLoop.getShiftBackRemaining();
                                if (skip) {
                                    shiftBackLoop.skip(file.getValueCount());
                                }
                                return skip;
                            }
                        });
                shiftBackLoop.loop(rangeValuesReverse);
                return new SingleValue(valueSerde, shiftBackLoop.getPrevValue());
            });
            return value.getValue(valueSerde);
        }
    }

    public V getNextValue(final FDate date, final int shiftForwardUnits) {
        switch (lookupMode) {
        case Value:
            return getNextValueByValue(date, shiftForwardUnits);
        case ValueUntilIndexAvailable:
            if (isLookupByIndexAvailable()) {
                return getNextValueByIndex(date, shiftForwardUnits);
            } else {
                return getNextValueByValue(date, shiftForwardUnits);
            }
        case Index:
            return getNextValueByIndex(date, shiftForwardUnits);
        default:
            throw UnknownArgumentException.newInstance(TimeSeriesLookupMode.class, lookupMode);
        }
    }

    private V getNextValueByIndex(final FDate date, final int shiftForwardUnits) {
        assertShiftUnitsPositiveNonZero(shiftForwardUnits);
        final V lastValue = getLastValue();
        if (lastValue == null) {
            return null;
        }
        final FDate lastTime = segmentedTable.extractEndTime(lastValue);
        if (date.isAfterOrEqualTo(lastTime)) {
            return lastValue;
        } else {
            final long valueIndex = nextValueIndexLookupCache.get(new RangeShiftUnitsKey(date, shiftForwardUnits));
            return getLatestValue(valueIndex);
        }
    }

    private long nextValueIndexLookup(final FDate date, final int shiftForwardUnits) {
        final AShiftForwardUnitsLoopLongIndex<V> shiftForwardLoop = new AShiftForwardUnitsLoopLongIndex<V>(date,
                shiftForwardUnits) {
            @Override
            protected V getLatestValue(final long index) {
                return ASegmentedTimeSeriesStorageCache.this.getLatestValue(index);
            }

            @Override
            protected long getLatestValueIndex(final FDate date) {
                return ASegmentedTimeSeriesStorageCache.this.getLatestValueIndex(date);
            }

            @Override
            protected FDate extractEndTime(final V value) {
                return segmentedTable.extractEndTime(value);
            }

            @Override
            protected long size() {
                return ASegmentedTimeSeriesStorageCache.this.size();
            }
        };
        shiftForwardLoop.loop();
        return shiftForwardLoop.getNextValueIndex();
    }

    private V getNextValueByValue(final FDate date, final int shiftForwardUnits) {
        assertShiftUnitsPositiveNonZero(shiftForwardUnits);
        final V lastValue = getLastValue();
        if (lastValue == null) {
            return null;
        }
        final FDate lastTime = segmentedTable.extractEndTime(lastValue);
        if (date.isAfterOrEqualTo(lastTime)) {
            return lastValue;
        } else {
            final SingleValue value = storage.getOrLoad_nextValueLookupTable(hashKey, date, shiftForwardUnits, () -> {
                final ShiftForwardUnitsLoop<V> shiftForwardLoop = new ShiftForwardUnitsLoop<>(date, shiftForwardUnits,
                        segmentedTable::extractEndTime);
                final ICloseableIterable<V> rangeValues = readRangeValues(date, null, DisabledLock.INSTANCE,
                        new ISkipFileFunction() {
                            @Override
                            public boolean skipFile(final MemoryFileSummary file) {
                                final boolean skip = shiftForwardLoop.getNextValue() != null
                                        && file.getValueCount() < shiftForwardLoop.getShiftForwardRemaining();
                                if (skip) {
                                    shiftForwardLoop.skip(file.getValueCount());
                                }
                                return skip;
                            }
                        });
                shiftForwardLoop.loop(rangeValues);
                return new SingleValue(valueSerde, shiftForwardLoop.getNextValue());
            });
            return value.getValue(valueSerde);
        }
    }

    private void maybePrepareForUpdate(final TimeRange segmentToBeInitialized) {
        try {
            deleteLock.lockInterruptibly();
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            final FDate maxLastAvailableSegmentToWithoutInitializing;
            if (segmentToBeInitialized == null) {
                maxLastAvailableSegmentToWithoutInitializing = getLastAvailableSegmentTo(key, null);
            } else {
                maxLastAvailableSegmentToWithoutInitializing = null;
            }
            final FDate prevLastAvailableSegmentTo = getPrevLastAvailableSegmentTo(
                    maxLastAvailableSegmentToWithoutInitializing);
            if (isNewSegmentAtTheEnd(maxLastAvailableSegmentToWithoutInitializing, prevLastAvailableSegmentTo,
                    segmentToBeInitialized)) {
                if (prevLastAvailableSegmentTo != null) {
                    storage.deleteRange_latestValueLookupTable(hashKey, prevLastAvailableSegmentTo);
                    storage.deleteRange_nextValueLookupTable(hashKey); //we cannot be sure here about the date since shift keys can be arbitrarily large
                    storage.deleteRange_previousValueLookupTable(hashKey, prevLastAvailableSegmentTo);
                }
                clearCaches();
            }
        } finally {
            deleteLock.unlock();
        }
    }

    private FDate getPrevLastAvailableSegmentTo(final FDate maxLastAvailableSegmentToWithoutLive) {
        if (maxLastAvailableSegmentToWithoutLive != null) {
            return getPrevLastAvailableSegmentToWithoutLive(maxLastAvailableSegmentToWithoutLive);
        } else {
            return getPrevLastAvailableSegmentToWithLive();
        }
    }

    private FDate getPrevLastAvailableSegmentToWithoutLive(final FDate maxLastAvailableSegmentToWithoutLive) {
        Optional<FDate> cachedPrevLastAvailableSegmentToWithoutLiveCopy = cachedPrevLastAvailableSegmentToWithoutLive;
        if (cachedPrevLastAvailableSegmentToWithoutLiveCopy == null) {
            RangeTableRow<String, TimeRange, SegmentStatus> latestRow = storage.getSegmentStatusTable()
                    .getLatest(hashKey);
            if (latestRow != null) {
                while (latestRow.getValue() == SegmentStatus.INITIALIZING
                        && maxLastAvailableSegmentToWithoutLive.isBeforeNotNullSafe(latestRow.getRangeKey().getTo())) {
                    //this must be a live segment which we are not interested in here
                    final RangeTableRow<String, TimeRange, SegmentStatus> prevRow = storage.getSegmentStatusTable()
                            .getLatest(hashKey, latestRow.getRangeKey().subtractDuration(Duration.ONE_MILLISECOND));
                    if (prevRow == null) {
                        //no earlier segment available
                        break;
                    }
                    if (prevRow.getRangeKey().getTo().isAfterOrEqualToNotNullSafe(latestRow.getRangeKey().getTo())) {
                        //no earlier segment available
                        break;
                    }
                    latestRow = prevRow;
                }
                cachedPrevLastAvailableSegmentToWithoutLiveCopy = Optional.of(latestRow.getRangeKey().getTo());
            } else {
                cachedPrevLastAvailableSegmentToWithoutLiveCopy = Optional.empty();
            }
            cachedPrevLastAvailableSegmentToWithoutLive = cachedPrevLastAvailableSegmentToWithoutLiveCopy;
        }
        return cachedPrevLastAvailableSegmentToWithoutLiveCopy.orElse(null);
    }

    private FDate getPrevLastAvailableSegmentToWithLive() {
        Optional<FDate> cachedPrevLastAvailableSegmentToWithLiveCopy = cachedPrevLastAvailableSegmentToWithLive;
        if (cachedPrevLastAvailableSegmentToWithLiveCopy == null) {
            final RangeTableRow<String, TimeRange, SegmentStatus> latestRow = storage.getSegmentStatusTable()
                    .getLatest(hashKey);
            if (latestRow != null) {
                cachedPrevLastAvailableSegmentToWithLiveCopy = Optional.of(latestRow.getRangeKey().getTo());
            } else {
                cachedPrevLastAvailableSegmentToWithLiveCopy = Optional.empty();
            }
            cachedPrevLastAvailableSegmentToWithLive = cachedPrevLastAvailableSegmentToWithLiveCopy;
        }
        return cachedPrevLastAvailableSegmentToWithLiveCopy.orElse(null);
    }

    private boolean isNewSegmentAtTheEnd(final FDate maxLastAvailableSegmentToWithoutLive,
            final FDate prevLastAvailableSegmentTo, final TimeRange segmentToBeInitialized) {
        if (prevLastAvailableSegmentTo == null) {
            return true;
        }
        final FDate lastAvailableSegmentTo;
        if (segmentToBeInitialized == null) {
            lastAvailableSegmentTo = maxLastAvailableSegmentToWithoutLive;
        } else {
            lastAvailableSegmentTo = getLastAvailableSegmentTo(key, segmentToBeInitialized.getTo());
        }
        if (lastAvailableSegmentTo == null) {
            return false;
        }
        if (segmentToBeInitialized == null) {
            /*
             * prevLastAvailableSegmentToWithLive could be higher than lastAvailableSegmentTo after a restart when live
             * segment was in initializing state, thus we have to make sure to compare against the last initialized
             * segment if available and can not do equals check here against prevLastAvailableSegmentToWithoutLive
             */
            return lastAvailableSegmentTo.isAfterNotNullSafe(prevLastAvailableSegmentTo);
        } else {
            return !lastAvailableSegmentTo.equals(prevLastAvailableSegmentTo)
                    && segmentToBeInitialized.getFrom().isAfter(prevLastAvailableSegmentTo);
        }
    }

    private void assertShiftUnitsPositiveNonZero(final int shiftUnits) {
        if (shiftUnits < 0) {
            throw new IllegalArgumentException("shiftUnits needs to be a positive or zero value: " + shiftUnits);
        }
    }

    public V getFirstValue() {
        if (cachedFirstValue != null) {
            maybePrepareForUpdate(null);
        }
        Optional<V> cachedFirstValueCopy = cachedFirstValue;
        if (cachedFirstValueCopy == null) {
            final FDate firstAvailableSegmentFrom = getFirstAvailableSegmentFrom(key);
            if (firstAvailableSegmentFrom == null) {
                cachedFirstValueCopy = Optional.empty();
            } else {
                FDate lastAvailableSegmentTo = getLastAvailableSegmentTo(key, null);
                final ISegmentFinder segmentFinder = getSegmentFinder(key);
                final TimeRange nextSegment = segmentFinder.getCacheQuery()
                        .getValue(lastAvailableSegmentTo.addMilliseconds(1));
                if (nextSegment.getFrom().equals(lastAvailableSegmentTo)) {
                    //adjust for overlapping segments
                    lastAvailableSegmentTo = lastAvailableSegmentTo.addMilliseconds(-1);
                }
                final TimeRange lastSegment = segmentFinder.getCacheQuery().getValue(lastAvailableSegmentTo);
                TimeRange segment = segmentFinder.getCacheQuery().getValue(firstAvailableSegmentFrom);
                if (!segment.getFrom().equalsNotNullSafe(firstAvailableSegmentFrom)) {
                    throw new IllegalStateException("segment.from [" + segment.getFrom()
                            + "] should be equal to firstAvailableSegmentFrom [" + firstAvailableSegmentFrom + "]");
                }
                while (cachedFirstValueCopy == null && segment.getFrom().isBeforeOrEqualTo(lastSegment.getFrom())) {
                    final SegmentedKey<K> segmentedKey = new SegmentedKey<K>(key, segment);
                    maybeInitSegment(segmentedKey);
                    final V potentialFirstValue = segmentedTable.getLookupTableCache(segmentedKey).getFirstValue();
                    final V firstValue;
                    if (potentialFirstValue == null) {
                        segment = segmentFinder.getCacheQuery().getValue(segment.getTo().addMilliseconds(1));
                    } else {
                        firstValue = potentialFirstValue;
                        cachedFirstValueCopy = Optional.of(firstValue);
                    }
                }
                if (cachedFirstValueCopy == null) {
                    cachedFirstValueCopy = Optional.empty();
                }
            }
            cachedFirstValue = cachedFirstValueCopy;
        }
        return cachedFirstValueCopy.orElse(null);
    }

    public V getLastValue() {
        if (cachedLastValue != null) {
            maybePrepareForUpdate(null);
        }
        Optional<V> cachedLastValueCopy = cachedLastValue;
        if (cachedLastValueCopy == null) {
            final FDate lastAvailableSegmentTo = getLastAvailableSegmentTo(key, null);
            if (lastAvailableSegmentTo == null) {
                cachedLastValueCopy = Optional.empty();
            } else {
                final V lastValue = getLatestValue(FDates.MAX_DATE);
                cachedLastValueCopy = Optional.ofNullable(lastValue);
            }
            cachedLastValue = cachedLastValueCopy;
        }
        return cachedLastValueCopy.orElse(null);
    }

    public boolean isLookupByIndexAvailable() {
        if (lookupByIndexAvailable) {
            return true;
        }
        synchronized (precedingValueCountCache) {
            if (lookupByIndexAvailable) {
                return true;
            }
            if (lookupByIndexAvailableFutureDisabled) {
                return false;
            }
            if (lookupByIndexAvailableFuture == null || lookupByIndexAvailableFuture.isDone()) {
                final FDate lastAvailableSegmentTo = getLastAvailableSegmentTo(key, null);
                if (lastAvailableSegmentTo == null) {
                    return false;
                }
                final long currentNanos = System.nanoTime();
                if (TimeSeriesProperties.ACQUIRE_UPDATE_LOCK_TIMEOUT
                        .isGreaterThanNanos(currentNanos - lookupByIndexAvailableFutureLastRunNanos)) {
                    return false;
                }
                lookupByIndexAvailableFutureLastRunNanos = currentNanos;
                lookupByIndexAvailableFuture = LOAD_INDEX_EXECUTOR.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            final ISegmentFinder segmentFinder = getSegmentFinder(key);
                            final TimeRange lastAvailableSegment = segmentFinder.getCacheQueryWithFuture()
                                    .getValue(lastAvailableSegmentTo.addMilliseconds(-1));
                            getPrecedingValueCount(new SegmentedKey<K>(key, lastAvailableSegment));
                            synchronized (precedingValueCountCache) {
                                lookupByIndexAvailable = true;
                            }
                        } catch (final Throwable t) {
                            if (Throwables.isCausedByInterrupt(t)) {
                                //ignore
                                return;
                            } else {
                                throw t;
                            }
                        }
                    }
                });
            }
        }
        return lookupByIndexAvailable;
    }

    public long getPrecedingValueCount(final SegmentedKey<K> beforeSegmentedKey) {
        /*
         * prevent deadlock with nested initSegment calls that clear the cache again, thus don't use computeIfAbsent or
         * a loadingCache
         */
        final Long existing = precedingValueCountCache.get(beforeSegmentedKey);
        if (existing != null) {
            return existing;
        }
        final long computed = newPrecedingValueCount(beforeSegmentedKey);
        precedingValueCountCache.put(beforeSegmentedKey, computed);
        return computed;
    }

    private long newPrecedingValueCount(final SegmentedKey<K> beforeSegmentedKey) {
        final FDate firstAvailableSegmentFrom = getFirstAvailableSegmentFrom(key);
        if (firstAvailableSegmentFrom == null) {
            return -1L;
        }
        final FDate lastAvailableSegmentTo = getLastAvailableSegmentTo(key, null);
        if (lastAvailableSegmentTo == null) {
            return -1L;
        }
        //adjust dates directly to prevent unnecessary segment calculations
        long precedingValueCount = 0L;
        final FDate adjFrom = firstAvailableSegmentFrom;
        final FDate adjTo = FDates.min(lastAvailableSegmentTo, beforeSegmentedKey.getSegment().getTo());
        final ISegmentFinder segmentFinder = getSegmentFinder(key);
        try (ICloseableIterator<TimeRange> segments = getSegments(segmentFinder, segmentFinder.getDay(adjFrom),
                segmentFinder.getDay(adjTo), lastAvailableSegmentTo).iterator()) {
            while (true) {
                final TimeRange value = segments.next();
                if (value.equals(beforeSegmentedKey.getSegment())) {
                    break;
                }
                //initialize all earlier segments eagerly so that indexes don't change during parallel queries by loading earlier segments
                final SegmentedKey<K> segmentedKey = new SegmentedKey<K>(key, value);
                maybeInitSegment(segmentedKey);
                precedingValueCount += segmentedTable.getLookupTableCache(segmentedKey).size();
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
        return precedingValueCount;
    }

    public long size() {
        long cachedSizeCopy = cachedSize;
        if (cachedSizeCopy == -1L) {
            long size = 0;
            try (ICloseableIterator<RangeTableRow<String, TimeRange, SegmentStatus>> rangeValues = storage
                    .getSegmentStatusTable()
                    .range(hashKey)) {
                while (true) {
                    final RangeTableRow<String, TimeRange, SegmentStatus> row = rangeValues.next();
                    final SegmentStatus status = row.getValue();
                    if (status == SegmentStatus.COMPLETE) {
                        final SegmentedKey<K> segmentedKey = new SegmentedKey<K>(key, row.getRangeKey());
                        size += segmentedTable.getLookupTableCache(segmentedKey).size();
                    }
                }
            } catch (final NoSuchElementException e) {
                //end reached
            }
            cachedSizeCopy = size;
            cachedSize = cachedSizeCopy;
        }
        return cachedSizeCopy;
    }

    public boolean isEmptyOrInconsistent() {
        try {
            getFirstValue();
            getLastValue();
        } catch (final Throwable t) {
            if (Throwables.isCausedByType(t, SerializationException.class)) {
                //e.g. fst: unable to find class for code 88 after version upgrade
                log.warn("Table data for [%s] is inconsistent and needs to be reset. Exception during getLastValue: %s",
                        hashKey, t.toString());
                return true;
            } else {
                //unexpected exception, since RemoteFastSerializingSerde only throws SerializingException
                throw Throwables.propagate(t);
            }
        }
        boolean empty = true;
        final ADelegateRangeTable<String, TimeRange, SegmentStatus> segmentStatusTable = storage
                .getSegmentStatusTable();
        final List<RangeTableRow<String, TimeRange, SegmentStatus>> rows;
        try (ICloseableIterator<RangeTableRow<String, TimeRange, SegmentStatus>> rangeKeysIterator = segmentStatusTable
                .range(hashKey)) {
            rows = Lists.toListWithoutHasNext(rangeKeysIterator);
        }
        for (int i = 0; i < rows.size(); i++) {
            final RangeTableRow<String, TimeRange, SegmentStatus> row = rows.get(i);
            final SegmentStatus status = row.getValue();
            if (status == SegmentStatus.COMPLETE) {
                if (segmentedTable.isEmptyOrInconsistent(new SegmentedKey<K>(key, row.getRangeKey()))) {
                    return true;
                }
            }
            empty = false;
        }
        return empty;
    }

    @Override
    public void close() {
        lookupByIndexAvailableFutureDisabled = true;
        final Future<?> future = lookupByIndexAvailableFuture;
        if (future != null) {
            future.cancel(true);
        }
        clearCaches();
        closed = true;
    }

    private final class LatestValueByIndexCache extends ALatestValueByIndexCache<V> {
        @Override
        protected long getLatestValueIndex(final FDate key) {
            return ASegmentedTimeSeriesStorageCache.this.getLatestValueIndex(key);
        }

        @Override
        protected V getLatestValue(final long index) {
            return ASegmentedTimeSeriesStorageCache.this.getLatestValue(index);
        }

        @Override
        protected FDate extractEndTime(final V value) {
            return segmentedTable.extractEndTime(value);
        }

        @Override
        protected int getLastResetIndex() {
            return lastResetIndex;
        }
    }

}
