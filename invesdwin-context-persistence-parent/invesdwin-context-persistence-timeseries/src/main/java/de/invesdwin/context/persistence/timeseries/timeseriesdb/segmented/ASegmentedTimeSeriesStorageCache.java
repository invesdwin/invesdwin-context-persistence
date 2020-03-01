package de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented;

import java.io.Closeable;
import java.io.OutputStream;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.mutable.MutableInt;
import org.springframework.retry.backoff.BackOffPolicy;

import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.integration.retry.task.ARetryCallable;
import de.invesdwin.context.integration.retry.task.BackOffPolicies;
import de.invesdwin.context.integration.retry.task.RetryOriginator;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.timeseries.ezdb.ADelegateRangeTable;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.IncompleteUpdateFoundException;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.TimeSeriesStorageCache;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.storage.ChunkValue;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.storage.ISkipFileFunction;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.storage.ShiftUnitsRangeKey;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.storage.SingleValue;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.updater.ALoggingTimeSeriesUpdater;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.updater.ITimeSeriesUpdater;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.bean.tuple.Pair;
import de.invesdwin.util.collections.eviction.EvictionMode;
import de.invesdwin.util.collections.iterable.ASkippingIterable;
import de.invesdwin.util.collections.iterable.ATransformingCloseableIterable;
import de.invesdwin.util.collections.iterable.ATransformingCloseableIterator;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterable;
import de.invesdwin.util.collections.iterable.FlatteningIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import de.invesdwin.util.collections.loadingcache.historical.AHistoricalCache;
import de.invesdwin.util.collections.loadingcache.historical.query.IHistoricalCacheQuery;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.Locks;
import de.invesdwin.util.concurrent.lock.disabled.DisabledLock;
import de.invesdwin.util.concurrent.lock.readwrite.IReadWriteLock;
import de.invesdwin.util.concurrent.reference.MutableReference;
import de.invesdwin.util.concurrent.taskinfo.provider.TaskInfoCallable;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FDates;
import de.invesdwin.util.time.range.TimeRange;
import ezdb.TableRow;
import ezdb.serde.Serde;
import net.jpountz.lz4.LZ4BlockOutputStream;

@NotThreadSafe
public abstract class ASegmentedTimeSeriesStorageCache<K, V> implements Closeable {
    public static final Integer MAXIMUM_SIZE = TimeSeriesStorageCache.MAXIMUM_SIZE;
    public static final EvictionMode EVICTION_MODE = TimeSeriesStorageCache.EVICTION_MODE;

    private final ALoadingCache<FDate, V> latestValueLookupCache = new ALoadingCache<FDate, V>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return MAXIMUM_SIZE;
        }

        @Override
        protected EvictionMode getEvictionMode() {
            return EVICTION_MODE;
        }

        @Override
        protected V loadValue(final FDate date) {
            final SingleValue value = storage.getLatestValueLookupTable()
                    .getOrLoad(hashKey, date, new Function<Pair<String, FDate>, SingleValue>() {

                        @Override
                        public SingleValue apply(final Pair<String, FDate> input) {
                            final FDate firstAvailableSegmentFrom = getFirstAvailableSegmentFrom(key);
                            //already adjusted on the outside
                            final FDate adjFrom = input.getSecond();
                            final FDate adjTo = firstAvailableSegmentFrom;
                            final ICloseableIterable<TimeRange> segmentsReverse = getSegmentsReverse(adjFrom, adjTo);
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
                                             * even if we got the first value in this segment and it is after the
                                             * desired key we just continue to the beginning to search for an earlier
                                             * value until we reach the overall firstValue
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
                        }
                    });
            if (value == null) {
                return null;
            }
            return value.getValue(valueSerde);
        }
    };
    private final ALoadingCache<Pair<FDate, Integer>, V> previousValueLookupCache = new ALoadingCache<Pair<FDate, Integer>, V>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return MAXIMUM_SIZE;
        }

        @Override
        protected EvictionMode getEvictionMode() {
            return EVICTION_MODE;
        }

        @Override
        protected V loadValue(final Pair<FDate, Integer> loadKey) {
            final FDate date = loadKey.getFirst();
            final int shiftBackUnits = loadKey.getSecond();
            final SingleValue value = storage.getPreviousValueLookupTable()
                    .getOrLoad(hashKey, new ShiftUnitsRangeKey(date, shiftBackUnits),
                            new Function<Pair<String, ShiftUnitsRangeKey>, SingleValue>() {

                                @Override
                                public SingleValue apply(final Pair<String, ShiftUnitsRangeKey> input) {
                                    final FDate date = loadKey.getFirst();
                                    final int shiftBackUnits = loadKey.getSecond();
                                    final MutableReference<V> previousValue = new MutableReference<>();
                                    final MutableInt shiftBackRemaining = new MutableInt(shiftBackUnits);
                                    try (ICloseableIterator<V> rangeValuesReverse = readRangeValuesReverse(date, null,
                                            DisabledLock.INSTANCE, new ISkipFileFunction() {
                                                @Override
                                                public boolean skipFile(final ChunkValue file) {
                                                    final boolean skip = previousValue.get() != null
                                                            && file.getCount() < shiftBackRemaining.intValue();
                                                    if (skip) {
                                                        shiftBackRemaining.add(file.getCount());
                                                    }
                                                    return skip;
                                                }
                                            }).iterator()) {
                                        while (shiftBackRemaining.intValue() > 0) {
                                            previousValue.set(rangeValuesReverse.next());
                                            shiftBackRemaining.decrement();
                                        }
                                    } catch (final NoSuchElementException e) {
                                        //ignore
                                    }
                                    return new SingleValue(valueSerde, previousValue.get());
                                }
                            });
            return value.getValue(valueSerde);
        }
    };
    private final ALoadingCache<Pair<FDate, Integer>, V> nextValueLookupCache = new ALoadingCache<Pair<FDate, Integer>, V>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return MAXIMUM_SIZE;
        }

        @Override
        protected EvictionMode getEvictionMode() {
            return EVICTION_MODE;
        }

        @Override
        protected V loadValue(final Pair<FDate, Integer> loadKey) {
            final FDate date = loadKey.getFirst();
            final int shiftForwardUnits = loadKey.getSecond();
            final SingleValue value = storage.getNextValueLookupTable()
                    .getOrLoad(hashKey, new ShiftUnitsRangeKey(date, shiftForwardUnits),
                            new Function<Pair<String, ShiftUnitsRangeKey>, SingleValue>() {

                                @Override
                                public SingleValue apply(final Pair<String, ShiftUnitsRangeKey> input) {
                                    final FDate date = loadKey.getFirst();
                                    final int shiftForwardUnits = loadKey.getSecond();
                                    final MutableReference<V> nextValue = new MutableReference<>();
                                    final MutableInt shiftForwardRemaining = new MutableInt(shiftForwardUnits);
                                    try (ICloseableIterator<V> rangeValues = readRangeValues(date, null,
                                            DisabledLock.INSTANCE, new ISkipFileFunction() {
                                                @Override
                                                public boolean skipFile(final ChunkValue file) {
                                                    final boolean skip = nextValue.get() != null
                                                            && file.getCount() < shiftForwardRemaining.intValue();
                                                    if (skip) {
                                                        shiftForwardRemaining.subtract(file.getCount());
                                                    }
                                                    return skip;
                                                }
                                            }).iterator()) {
                                        while (shiftForwardRemaining.intValue() > 0) {
                                            nextValue.set(rangeValues.next());
                                            shiftForwardRemaining.decrement();
                                        }
                                    } catch (final NoSuchElementException e) {
                                        //ignore
                                    }
                                    return new SingleValue(valueSerde, nextValue.get());
                                }
                            });
            return value.getValue(valueSerde);
        }
    };

    private volatile boolean closed;
    private volatile Optional<V> cachedFirstValue;
    private volatile Optional<V> cachedLastValue;
    private volatile Optional<FDate> cachedPrevLastAvailableSegmentTo;
    private final Log log = new Log(this);

    private final ASegmentedTimeSeriesDB<K, V>.SegmentedTable segmentedTable;
    private final SegmentedTimeSeriesStorage storage;
    private final K key;
    private final String hashKey;
    private final Serde<V> valueSerde;
    private final Function<SegmentedKey<K>, ICloseableIterable<? extends V>> source;

    public ASegmentedTimeSeriesStorageCache(final ASegmentedTimeSeriesDB<K, V>.SegmentedTable segmentedTable,
            final SegmentedTimeSeriesStorage storage, final K key, final String hashKey) {
        this.storage = storage;
        this.segmentedTable = segmentedTable;
        this.key = key;
        this.hashKey = hashKey;
        this.valueSerde = segmentedTable.getValueSerde();
        this.source = new Function<SegmentedKey<K>, ICloseableIterable<? extends V>>() {
            @Override
            public ICloseableIterable<? extends V> apply(final SegmentedKey<K> t) {
                return downloadSegmentElements(t);
            }
        };
    }

    public ICloseableIterable<V> readRangeValues(final FDate from, final FDate to, final Lock readLock,
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
        final ICloseableIterable<TimeRange> segments = getSegments(adjFrom, adjTo);
        final ATransformingCloseableIterable<TimeRange, ICloseableIterable<V>> segmentQueries = new ATransformingCloseableIterable<TimeRange, ICloseableIterable<V>>(
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
                        final Lock compositeReadLock = Locks.newCompositeLock(readLock,
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

    private ICloseableIterable<TimeRange> getSegments(final FDate from, final FDate to) {
        if (from == null || to == null) {
            return EmptyCloseableIterable.getInstance();
        }
        final TimeRange nextSegment = getSegmentFinder(key).query().getValue(to.addMilliseconds(1));
        final FDate adjTo;
        if (nextSegment.getFrom().equals(to)) {
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

                    private TimeRange nextSegment = getSegmentFinder(key).query().getValue(adjFrom);

                    @Override
                    public boolean hasNext() {
                        return nextSegment != null && nextSegment.getFrom().isBeforeOrEqualTo(adjTo);
                    }

                    @Override
                    public TimeRange next() {
                        final TimeRange curSegment = nextSegment;
                        //get one segment later
                        final FDate nextSegmentStart = nextSegment.getTo().addMilliseconds(1);
                        nextSegment = getSegmentFinder(key).query().getValue(nextSegmentStart);
                        if (!curSegment.getTo().equalsNotNullSafe(nextSegment.getFrom())
                                && !nextSegmentStart.equals(nextSegment.getFrom())) {
                            //allow overlapping segments
                            throw new IllegalStateException("Segment start expected [" + curSegment.getTo() + " or "
                                    + nextSegmentStart + "] != found [" + nextSegment.getFrom() + "]");
                        }
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
                if (segmentTo.isBefore(adjFrom)) {
                    throw new IllegalStateException(
                            "segmentTo [" + segmentTo + "] should not be before adjFrom [" + adjFrom + "]");
                }
                final FDate segmentFrom = element.getFrom();
                if (segmentFrom.isAfter(adjTo)) {
                    //no need to continue going higher
                    throw new FastNoSuchElementException("ASegmentedTimeSeriesStorageCache getSegments end reached");
                }
                return false;
            }
        };
        return filteredSegments;
    }

    protected abstract AHistoricalCache<TimeRange> getSegmentFinder(K key);

    public void maybeInitSegment(final SegmentedKey<K> segmentedKey) {
        maybeInitSegment(segmentedKey, source);
    }

    public boolean maybeInitSegment(final SegmentedKey<K> segmentedKey,
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
                    if (!segmentWriteLock.tryLock(1, TimeUnit.MINUTES)) {
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
        if (firstAvailableSegmentFrom.isAfter(lastAvailableSegmentTo)) {
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
        if (segmentTo.isAfter(lastAvailableSegmentTo)) {
            throw new IllegalStateException(segmentedKey + ": segmentTo [" + segmentTo
                    + "] should not be after lastAvailableSegmentTo [" + lastAvailableSegmentTo + "]");
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
            final ReadWriteLock segmentTableLock) {
        final Lock segmentReadLock = segmentTableLock.readLock();
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
                                + "for [" + hashKey + "] is already closed.");
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
                final TimeRange prevSegment = getSegmentFinder(segmentedKey.getKey()).query()
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
            if (Throwables.isCausedByType(t, IncompleteUpdateFoundException.class)) {
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
                    Assertions.checkNull(updateFrom);
                    return source.apply(segmentedKey);
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
                protected LZ4BlockOutputStream newCompressor(final OutputStream out) {
                    return ASegmentedTimeSeriesStorageCache.this.newCompressor(out);
                }

                @Override
                public Percent getProgress() {
                    final FDate estimatedTo = segmentedKey.getSegment().getTo();
                    final FDate from = getMinTime();
                    if (from == null) {
                        return null;
                    }
                    final FDate curTime = getMaxTime();
                    if (curTime == null) {
                        return null;
                    }
                    return new Percent(new Duration(from, curTime), new Duration(from, estimatedTo))
                            .orLower(Percent.ONE_HUNDRED_PERCENT);
                }
            };
        }
        return updater;
    }

    public abstract void onSegmentCompleted(SegmentedKey<K> segmentedKey, ICloseableIterable<V> segmentValues);

    protected abstract ITimeSeriesUpdater<SegmentedKey<K>, V> newSegmentUpdaterOverride(SegmentedKey<K> segmentedKey,
            ASegmentedTimeSeriesDB<K, V>.SegmentedTable segmentedTable,
            Function<SegmentedKey<K>, ICloseableIterable<? extends V>> source);

    protected abstract String getElementsName();

    protected abstract LZ4BlockOutputStream newCompressor(OutputStream out);

    protected abstract ICloseableIterable<? extends V> downloadSegmentElements(SegmentedKey<K> segmentedKey);

    protected abstract FDate getLastAvailableSegmentTo(K key, FDate updateTo);

    protected abstract FDate getFirstAvailableSegmentFrom(K key);

    public ICloseableIterable<V> readRangeValuesReverse(final FDate from, final FDate to, final Lock readLock,
            final ISkipFileFunction skipFileFunction) {
        final FDate firstAvailableSegmentFrom = getFirstAvailableSegmentFrom(key);
        final FDate lastAvailableSegmentTo = getLastAvailableSegmentTo(key, to);
        //adjust dates directly to prevent unnecessary segment calculations
        final FDate adjFrom = FDates.min(from, lastAvailableSegmentTo);
        final FDate adjTo = FDates.max(to, firstAvailableSegmentFrom);
        final ICloseableIterable<TimeRange> filteredSegments = getSegmentsReverse(adjFrom, adjTo);
        final ATransformingCloseableIterable<TimeRange, ICloseableIterable<V>> segmentQueries = new ATransformingCloseableIterable<TimeRange, ICloseableIterable<V>>(
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
                        final Lock compositeReadLock = Locks.newCompositeLock(readLock,
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

    private ICloseableIterable<TimeRange> getSegmentsReverse(final FDate from, final FDate to) {
        if (from == null || to == null) {
            return EmptyCloseableIterable.getInstance();
        }
        final TimeRange nextSegment = getSegmentFinder(key).query().getValue(from.addMilliseconds(1));
        final FDate adjFrom;
        if (nextSegment.getFrom().equals(to)) {
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

                    private TimeRange nextSegment = getSegmentFinder(key).query().getValue(adjFrom);

                    @Override
                    public boolean hasNext() {
                        return nextSegment != null && nextSegment.getTo().isAfter(adjTo);
                    }

                    @Override
                    public TimeRange next() {
                        final TimeRange curSegment = nextSegment;
                        if (curSegment == null) {
                            throw new FastNoSuchElementException(
                                    "ASegmentedTimeSeriesStorageCache getSegments end reached null");
                        }
                        //get one segment earlier
                        nextSegment = getSegmentFinder(key).query()
                                .withFutureNull()
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
                    throw new FastNoSuchElementException(
                            "ASegmentedTimeSeriesStorageCache getSegments end reached adjTo");
                }
                //skip last value and continue with earlier ones
                final FDate segmentFrom = element.getFrom();
                return segmentFrom.isAfter(adjFrom);
            }
        };
        return filteredSegments;
    }

    public synchronized void deleteAll() {
        final ADelegateRangeTable<String, TimeRange, SegmentStatus> segmentStatusTable = storage
                .getSegmentStatusTable();
        final List<TimeRange> rangeKeys;
        try (ICloseableIterator<TimeRange> rangeKeysIterator = new ATransformingCloseableIterator<TableRow<String, TimeRange, SegmentStatus>, TimeRange>(
                segmentStatusTable.range(hashKey)) {

            @Override
            protected TimeRange transform(final TableRow<String, TimeRange, SegmentStatus> value) {
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
        storage.getLatestValueLookupTable().deleteRange(hashKey);
        storage.getNextValueLookupTable().deleteRange(hashKey);
        storage.getPreviousValueLookupTable().deleteRange(hashKey);
        clearCaches();
    }

    private void clearCaches() {
        latestValueLookupCache.clear();
        nextValueLookupCache.clear();
        previousValueLookupCache.clear();
        cachedFirstValue = null;
        cachedLastValue = null;
        cachedPrevLastAvailableSegmentTo = null;
    }

    public V getLatestValue(final FDate date) {
        final FDate lastAvailableSegmentTo = getLastAvailableSegmentTo(key, date);
        final FDate adjDate = FDates.min(date, lastAvailableSegmentTo);
        return latestValueLookupCache.get(adjDate);
    }

    public V getPreviousValue(final FDate date, final int shiftBackUnits) {
        assertShiftUnitsPositiveNonZero(shiftBackUnits);
        final V firstValue = getFirstValue();
        final FDate firstTime = segmentedTable.extractEndTime(firstValue);
        if (date.isBeforeOrEqualTo(firstTime)) {
            return firstValue;
        } else {
            return previousValueLookupCache.get(Pair.of(date, shiftBackUnits));
        }
    }

    public V getNextValue(final FDate date, final int shiftForwardUnits) {
        assertShiftUnitsPositiveNonZero(shiftForwardUnits);
        final V lastValue = getLastValue();
        final FDate lastTime = segmentedTable.extractEndTime(lastValue);
        if (date.isAfterOrEqualTo(lastTime)) {
            return lastValue;
        } else {
            return nextValueLookupCache.get(Pair.of(date, shiftForwardUnits));
        }
    }

    private synchronized void maybePrepareForUpdate(final TimeRange segmentToBeInitialized) {
        final FDate prevLastAvailableSegmentTo = getPrevLastAvailableSegmentTo();
        if (isNewSegmentAtTheEnd(prevLastAvailableSegmentTo, segmentToBeInitialized)) {
            if (prevLastAvailableSegmentTo != null) {
                storage.getLatestValueLookupTable().deleteRange(hashKey, prevLastAvailableSegmentTo);
                storage.getNextValueLookupTable().deleteRange(hashKey); //we cannot be sure here about the date since shift keys can be arbitrarily large
                storage.getPreviousValueLookupTable()
                        .deleteRange(hashKey, new ShiftUnitsRangeKey(prevLastAvailableSegmentTo, 0));
            }
            clearCaches();
        }
    }

    private FDate getPrevLastAvailableSegmentTo() {
        if (cachedPrevLastAvailableSegmentTo == null) {
            final TableRow<String, TimeRange, SegmentStatus> latestRow = storage.getSegmentStatusTable()
                    .getLatest(hashKey);
            if (latestRow != null) {
                cachedPrevLastAvailableSegmentTo = Optional.of(latestRow.getRangeKey().getTo());
            } else {
                cachedPrevLastAvailableSegmentTo = Optional.empty();
            }
        }
        return cachedPrevLastAvailableSegmentTo.orElse(null);
    }

    private boolean isNewSegmentAtTheEnd(final FDate prevLastAvailableSegmentTo,
            final TimeRange segmentToBeInitialized) {
        if (prevLastAvailableSegmentTo == null) {
            return true;
        }
        final FDate lastAvailableSegmentTo;
        if (segmentToBeInitialized == null) {
            lastAvailableSegmentTo = getLastAvailableSegmentTo(key, null);
        } else {
            lastAvailableSegmentTo = getLastAvailableSegmentTo(key, segmentToBeInitialized.getTo());
        }
        if (lastAvailableSegmentTo == null) {
            return false;
        }
        return !lastAvailableSegmentTo.equals(prevLastAvailableSegmentTo) && (segmentToBeInitialized == null
                || segmentToBeInitialized.getFrom().isAfter(prevLastAvailableSegmentTo));
    }

    private void assertShiftUnitsPositiveNonZero(final int shiftUnits) {
        if (shiftUnits <= 0) {
            throw new IllegalArgumentException("shiftUnits needs to be a positive non zero value: " + shiftUnits);
        }
    }

    public V getFirstValue() {
        if (cachedFirstValue != null) {
            maybePrepareForUpdate(null);
        }
        if (cachedFirstValue == null) {
            final FDate firstAvailableSegmentFrom = getFirstAvailableSegmentFrom(key);
            if (firstAvailableSegmentFrom == null) {
                cachedFirstValue = Optional.empty();
            } else {
                final FDate lastAvailableSegmentTo = getLastAvailableSegmentTo(key, null);
                final IHistoricalCacheQuery<TimeRange> segmentFinderQuery = getSegmentFinder(key).query();
                final TimeRange lastSegment = segmentFinderQuery.getValue(lastAvailableSegmentTo);
                TimeRange segment = segmentFinderQuery.getValue(firstAvailableSegmentFrom);
                Assertions.assertThat(segment.getFrom()).isEqualTo(firstAvailableSegmentFrom);
                while (cachedFirstValue == null && segment.getFrom().isBeforeOrEqualTo(lastSegment.getFrom())) {
                    final SegmentedKey<K> segmentedKey = new SegmentedKey<K>(key, segment);
                    maybeInitSegment(segmentedKey);
                    final String segmentedHashKey = segmentedTable.hashKeyToString(segmentedKey);
                    final ChunkValue latestValue = storage.getFileLookupTable()
                            .getLatestValue(segmentedHashKey, FDate.MIN_DATE);
                    final V firstValue;
                    if (latestValue == null) {
                        segment = segmentFinderQuery.getValue(segment.getTo().addMilliseconds(1));
                    } else {
                        firstValue = latestValue.getFirstValue(valueSerde);
                        cachedFirstValue = Optional.of(firstValue);
                    }
                }
                if (cachedFirstValue == null) {
                    cachedFirstValue = Optional.empty();
                }
            }
        }
        return cachedFirstValue.orElse(null);
    }

    public V getLastValue() {
        if (cachedLastValue != null) {
            maybePrepareForUpdate(null);
        }
        if (cachedLastValue == null) {
            final FDate lastAvailableSegmentTo = getLastAvailableSegmentTo(key, null);
            if (lastAvailableSegmentTo == null) {
                cachedLastValue = Optional.empty();
            } else {
                final V lastValue = getLatestValue(FDate.MAX_DATE);
                cachedLastValue = Optional.ofNullable(lastValue);
            }
        }
        return cachedLastValue.orElse(null);
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
        final List<TableRow<String, TimeRange, SegmentStatus>> rows;
        try (ICloseableIterator<TableRow<String, TimeRange, SegmentStatus>> rangeKeysIterator = segmentStatusTable
                .range(hashKey)) {
            rows = Lists.toListWithoutHasNext(rangeKeysIterator);
        }
        for (int i = 0; i < rows.size(); i++) {
            final TableRow<String, TimeRange, SegmentStatus> row = rows.get(i);
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
        clearCaches();
        closed = true;
    }

}
