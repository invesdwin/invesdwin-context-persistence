package de.invesdwin.context.persistence.timeseriesdb.segmented.live;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.loop.AShiftForwardUnitsLoopLongIndex;
import de.invesdwin.context.persistence.timeseriesdb.loop.ShiftBackUnitsLoop;
import de.invesdwin.context.persistence.timeseriesdb.segmented.ISegmentedTimeSeriesDBInternals;
import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseriesdb.segmented.finder.ISegmentFinder;
import de.invesdwin.context.persistence.timeseriesdb.segmented.live.segment.ILiveSegment;
import de.invesdwin.context.persistence.timeseriesdb.segmented.live.segment.ReadLockedLiveSegment;
import de.invesdwin.context.persistence.timeseriesdb.segmented.live.segment.SwitchingLiveSegment;
import de.invesdwin.context.persistence.timeseriesdb.storage.ISkipFileFunction;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.collections.iterable.FlatteningIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.Locks;
import de.invesdwin.util.concurrent.lock.disabled.DisabledLock;
import de.invesdwin.util.concurrent.lock.readwrite.IReadWriteLock;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.range.TimeRange;

@ThreadSafe
public abstract class ALiveSegmentedTimeSeriesStorageCache<K, V> implements Closeable {

    private final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable;
    private final K key;
    private final IReadWriteLock liveSegmentLock;
    private volatile ReadLockedLiveSegment<K, V> liveSegment;
    private final Function<FDate, V> liveSegmentLatestValueProvider = new Function<FDate, V>() {
        @Override
        public V apply(final FDate t) {
            return liveSegment.getLatestValue(t);
        }
    };
    private final Function<FDate, V> historicalSegmentLatestValueProvider = new Function<FDate, V>() {
        @Override
        public V apply(final FDate t) {
            return historicalSegmentTable.getLatestValue(key, t);
        }
    };
    @SuppressWarnings("unchecked")
    private final List<Function<FDate, V>> latestValueProviders = Arrays.asList(liveSegmentLatestValueProvider,
            historicalSegmentLatestValueProvider);
    private final ToLongFunction<FDate> liveSegmentLatestValueIndexProvider = new ToLongFunction<FDate>() {
        @Override
        public long applyAsLong(final FDate value) {
            final long latestValueIndex = liveSegment.getLatestValueIndex(value);
            if (latestValueIndex == -1L) {
                return -1L;
            }
            return latestValueIndex + historicalSegmentTable.size(key);
        }
    };
    private final ToLongFunction<FDate> historicalSegmentLatestValueIndexProvider = new ToLongFunction<FDate>() {
        @Override
        public long applyAsLong(final FDate value) {
            return historicalSegmentTable.getLatestValueIndex(key, value);
        }
    };
    @SuppressWarnings("unchecked")
    private final List<ToLongFunction<FDate>> latestValueIndexProviders = Arrays
            .asList(liveSegmentLatestValueIndexProvider, historicalSegmentLatestValueIndexProvider);
    private final int batchFlushInterval;

    public ALiveSegmentedTimeSeriesStorageCache(
            final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable, final K key,
            final int batchFlushInterval) {
        this.historicalSegmentTable = historicalSegmentTable;
        this.key = key;
        this.batchFlushInterval = batchFlushInterval;
        this.liveSegmentLock = Locks
                .newReentrantReadWriteLock("liveSegmentLock_" + historicalSegmentTable.hashKeyToString(key));
    }

    public boolean isEmptyOrInconsistent() {
        if (liveSegment != null && liveSegment.isEmpty()) {
            return true;
        }
        return historicalSegmentTable.isEmptyOrInconsistent(key);
    }

    public void deleteAll() {
        if (liveSegment != null) {
            liveSegment.close();
        }
        liveSegment = null;
        historicalSegmentTable.deleteRange(key);
    }

    public V getFirstValue() {
        final V firstHistoricalValue = historicalSegmentTable.getLatestValue(key, FDates.MIN_DATE);
        if (firstHistoricalValue != null) {
            return firstHistoricalValue;
        } else if (liveSegment != null) {
            return liveSegment.getFirstValue();
        }
        return null;
    }

    public V getLastValue() {
        if (liveSegment != null) {
            final V lastLiveValue = liveSegment.getLastValue();
            if (lastLiveValue != null) {
                return lastLiveValue;
            }
        }
        return historicalSegmentTable.getLatestValue(key, FDates.MAX_DATE);
    }

    public ICloseableIterable<V> readRangeValues(final FDate from, final FDate to, final Lock readLock,
            final ISkipFileFunction skipFileFunction) {
        readLock.lock();
        try {
            if (liveSegment == null) {
                //no live segment, go with historical
                final Lock compositeReadLock = Locks.newCompositeLock(readLock,
                        historicalSegmentTable.getTableLock(key).readLock());
                return historicalSegmentTable.getSegmentedLookupTableCache(key)
                        .readRangeValues(from, to, compositeReadLock, skipFileFunction);
            } else {
                final FDate liveSegmentFrom = liveSegment.getSegmentedKey().getSegment().getFrom();
                if (liveSegmentFrom.isAfter(to)) {
                    //live segment is after requested range, go with historical
                    final Lock compositeReadLock = Locks.newCompositeLock(readLock,
                            historicalSegmentTable.getTableLock(key).readLock());
                    return historicalSegmentTable.getSegmentedLookupTableCache(key)
                            .readRangeValues(from, to, compositeReadLock, skipFileFunction);
                } else if (liveSegmentFrom.isBeforeOrEqualTo(from)) {
                    //historical segment is before requested range, go with live
                    return liveSegment.rangeValues(from, to, readLock, skipFileFunction);
                } else {
                    //use both segments
                    final Lock compositeReadLock = Locks.newCompositeLock(readLock,
                            historicalSegmentTable.getTableLock(key).readLock());
                    final ICloseableIterable<V> historicalRangeValues = historicalSegmentTable
                            .getSegmentedLookupTableCache(key)
                            .readRangeValues(from, liveSegmentFrom.addMilliseconds(-1), compositeReadLock,
                                    skipFileFunction);
                    final ICloseableIterable<V> liveRangeValues = liveSegment.rangeValues(liveSegmentFrom, to, readLock,
                            skipFileFunction);
                    return new FlatteningIterable<V>(historicalRangeValues, liveRangeValues);
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    public ICloseableIterable<V> readRangeValuesReverse(final FDate from, final FDate to, final Lock readLock,
            final ISkipFileFunction skipFileFunction) {
        readLock.lock();
        try {
            if (liveSegment == null) {
                //no live segment, go with historical
                final Lock compositeReadLock = Locks.newCompositeLock(readLock,
                        historicalSegmentTable.getTableLock(key).readLock());
                return historicalSegmentTable.getSegmentedLookupTableCache(key)
                        .readRangeValuesReverse(from, to, compositeReadLock, skipFileFunction);
            } else {
                final FDate liveSegmentFrom = liveSegment.getSegmentedKey().getSegment().getFrom();
                if (liveSegmentFrom.isAfter(from)) {
                    //live segment is after requested range, go with historical
                    final Lock compositeReadLock = Locks.newCompositeLock(readLock,
                            historicalSegmentTable.getTableLock(key).readLock());
                    return historicalSegmentTable.getSegmentedLookupTableCache(key)
                            .readRangeValuesReverse(from, to, compositeReadLock, skipFileFunction);
                } else if (liveSegmentFrom.isBeforeOrEqualTo(to)) {
                    //historical segment is before requested range, go with live
                    return liveSegment.rangeReverseValues(from, to, readLock, skipFileFunction);
                } else {
                    //use both segments
                    final ICloseableIterable<V> liveRangeValues = liveSegment.rangeReverseValues(from, liveSegmentFrom,
                            readLock, skipFileFunction);
                    final Lock compositeReadLock = Locks.newCompositeLock(readLock,
                            historicalSegmentTable.getTableLock(key).readLock());
                    final ICloseableIterable<V> historicalRangeValues = historicalSegmentTable
                            .getSegmentedLookupTableCache(key)
                            .readRangeValuesReverse(liveSegmentFrom.addMilliseconds(-1), to, compositeReadLock,
                                    skipFileFunction);
                    return new FlatteningIterable<V>(liveRangeValues, historicalRangeValues);
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    public V getLatestValue(final FDate date) {
        if (liveSegment == null) {
            return historicalSegmentLatestValueProvider.apply(date);
        }
        V latestValue = null;
        for (int i = 0; i < latestValueProviders.size(); i++) {
            final Function<FDate, V> latestValueProvider = latestValueProviders.get(i);
            final V newValue = latestValueProvider.apply(date);
            if (newValue != null) {
                final FDate newValueTime = historicalSegmentTable.extractEndTime(newValue);
                if (newValueTime.isBeforeOrEqualTo(date)) {
                    /*
                     * even if we got the first value in this segment and it is after the desired key we just continue
                     * to the beginning to search for an earlier value until we reach the overall firstValue
                     */
                    latestValue = newValue;
                    break;
                }
            }
        }
        if (latestValue == null) {
            latestValue = getFirstValue();
        }
        return latestValue;
    }

    public V getLatestValue(final long index) {
        if (index < historicalSegmentTable.size(key)) {
            return historicalSegmentTable.getLatestValue(key, index);
        } else if (liveSegment == null) {
            return historicalSegmentTable.getLatestValue(key, FDates.MAX_DATE);
        } else {
            final long liveSegmentIndex = index - historicalSegmentTable.size(key);
            return liveSegment.getLatestValue(liveSegmentIndex);
        }
    }

    public long getLatestValueIndex(final FDate date) {
        if (liveSegment == null) {
            return historicalSegmentLatestValueIndexProvider.applyAsLong(date);
        }
        long latestValueIndex = -1L;
        for (int i = 0; i < latestValueIndexProviders.size(); i++) {
            final ToLongFunction<FDate> latestValueIndexProvider = latestValueIndexProviders.get(i);
            final long newValueIndex = latestValueIndexProvider.applyAsLong(date);
            if (newValueIndex != -1L) {
                final V newValue = getLatestValue(newValueIndex);
                final FDate newValueTime = historicalSegmentTable.extractEndTime(newValue);
                if (newValueTime.isBeforeOrEqualTo(date)) {
                    /*
                     * even if we got the first value in this segment and it is after the desired key we just continue
                     * to the beginning to search for an earlier value until we reach the overall firstValue
                     */
                    latestValueIndex = newValueIndex;
                    break;
                }
            }
        }
        if (latestValueIndex == -1L && getFirstValue() != null) {
            return 0L;
        }
        return latestValueIndex;
    }

    public long size() {
        if (liveSegment == null) {
            return historicalSegmentTable.size(key);
        } else {
            return historicalSegmentTable.size(key) + liveSegment.size();
        }
    }

    public V getPreviousValue(final FDate date, final int shiftBackUnits) {
        if (liveSegment == null) {
            //no live segment, go with historical
            return historicalSegmentTable.getPreviousValue(key, date, shiftBackUnits);
        } else if (liveSegment.getSegmentedKey().getSegment().getFrom().isAfter(date)) {
            //live segment is after requested range, go with historical
            return historicalSegmentTable.getPreviousValue(key, date, shiftBackUnits);
        } else {
            final ShiftBackUnitsLoop<V> shiftBackLoop = new ShiftBackUnitsLoop<>(date, shiftBackUnits,
                    historicalSegmentTable::extractEndTime);
            final ICloseableIterable<V> rangeValuesReverse = readRangeValuesReverse(date, null, DisabledLock.INSTANCE,
                    file -> {
                        final boolean skip = shiftBackLoop.getPrevValue() != null
                                && file.getValueCount() < shiftBackLoop.getShiftBackRemaining();
                        if (skip) {
                            shiftBackLoop.skip(file.getValueCount());
                        }
                        return skip;
                    });
            shiftBackLoop.loop(rangeValuesReverse);
            return shiftBackLoop.getPrevValue();
        }
    }

    public V getNextValue(final FDate date, final int shiftForwardUnits) {
        if (liveSegment == null) {
            //no live segment, go with historical
            return historicalSegmentTable.getNextValue(key, date, shiftForwardUnits);
        } else if (liveSegment.getSegmentedKey().getSegment().getFrom().isBefore(date)) {
            //live segment is after requested range, go with live
            final V nextValue = liveSegment.getNextValue(date, shiftForwardUnits);
            return nextValue;
        } else {
            //use both segments
            final AShiftForwardUnitsLoopLongIndex<V> shiftForwardLoop = new AShiftForwardUnitsLoopLongIndex<V>(date,
                    shiftForwardUnits) {
                @Override
                protected V getLatestValue(final long index) {
                    return ALiveSegmentedTimeSeriesStorageCache.this.getLatestValue(index);
                }

                @Override
                protected long getLatestValueIndex(final FDate date) {
                    return ALiveSegmentedTimeSeriesStorageCache.this.getLatestValueIndex(date);
                }

                @Override
                protected FDate extractEndTime(final V value) {
                    return historicalSegmentTable.extractEndTime(value);
                }

                @Override
                protected long size() {
                    return ALiveSegmentedTimeSeriesStorageCache.this.size();
                }
            };
            shiftForwardLoop.loop();
            return shiftForwardLoop.getNextValue();
        }
    }

    public void putNextLiveValue(final V nextLiveValue) {
        final ILock liveWriteLock = liveSegmentLock.writeLock();
        liveWriteLock.lock();
        try {
            final FDate nextLiveKey = historicalSegmentTable.extractEndTime(nextLiveValue);
            final FDate lastAvailableHistoricalSegmentTo = historicalSegmentTable
                    .getLastAvailableHistoricalSegmentTo(key, nextLiveKey);
            final ISegmentFinder segmentFinder = historicalSegmentTable.getSegmentFinder(key);
            final TimeRange segment = segmentFinder.getCacheQuery().getValue(segmentFinder.getDay(nextLiveKey));
            if (lastAvailableHistoricalSegmentTo.isAfterNotNullSafe(segment.getFrom())
                    /*
                     * allow equals since on first value of the next bar we might get an overlap for once when the last
                     * available time was updated beforehand
                     */
                    && !lastAvailableHistoricalSegmentTo.equalsNotNullSafe(segment.getTo())) {
                throw new IllegalStateException("lastAvailableHistoricalSegmentTo [" + lastAvailableHistoricalSegmentTo
                        + "] should be before or equal to liveSegmentFrom [" + segment.getFrom() + "]");
            }
            if (liveSegment != null && nextLiveKey.isAfter(liveSegment.getSegmentedKey().getSegment().getTo())) {
                if (!lastAvailableHistoricalSegmentTo
                        .isBeforeOrEqualTo(liveSegment.getSegmentedKey().getSegment().getTo())) {
                    throw new IllegalStateException(
                            "lastAvailableHistoricalSegmentTo [" + lastAvailableHistoricalSegmentTo
                                    + "] should be before or equal to liveSegmentTo [" + segment.getTo() + "]");
                }
                liveSegment.convertLiveSegmentToHistorical();
                liveSegment.close();
                liveSegment = null;
            }
            if (liveSegment == null) {
                final SegmentedKey<K> segmentedKey = new SegmentedKey<K>(key, segment);
                liveSegment = new ReadLockedLiveSegment<K, V>(
                        newLiveSegment(segmentedKey, historicalSegmentTable, batchFlushInterval),
                        liveSegmentLock.readLock());
            }
            liveSegment.putNextLiveValue(nextLiveKey, nextLiveValue);
        } finally {
            liveWriteLock.unlock();
        }
    }

    protected ILiveSegment<K, V> newLiveSegment(final SegmentedKey<K> segmentedKey,
            final ISegmentedTimeSeriesDBInternals<K, V> historicalSegmentTable, final int batchFlushInterval) {
        return newDefaultLiveSegment(segmentedKey, historicalSegmentTable, batchFlushInterval);
    }

    public static <K, V> ILiveSegment<K, V> newDefaultLiveSegment(final SegmentedKey<K> segmentedKey,
            final ISegmentedTimeSeriesDBInternals<K, V> historicalSegmentTable, final int batchFlushInterval) {
        return new SwitchingLiveSegment<>(segmentedKey, historicalSegmentTable, batchFlushInterval);
    }

    @Override
    public void close() {
        if (liveSegment != null) {
            liveSegment.close();
        }
    }

}
