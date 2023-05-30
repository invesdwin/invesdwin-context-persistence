package de.invesdwin.context.persistence.timeseriesdb.segmented.live.internal;

import java.io.File;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.ezdb.table.range.ADelegateRangeTable;
import de.invesdwin.context.persistence.timeseriesdb.segmented.ASegmentedTimeSeriesStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseriesdb.segmented.live.ALiveSegmentedTimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.storage.ISkipFileFunction;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import de.invesdwin.util.concurrent.lock.disabled.DisabledLock;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.marshallers.serde.basic.VoidSerde;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class RangeTableLiveSegment<K, V> implements ILiveSegment<K, V> {

    private static final Log LOG = new Log(RangeTableLiveSegment.class);
    private final SegmentedKey<K> segmentedKey;
    private final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable;
    private final ADelegateRangeTable<Void, FDate, V> values;
    private FDate firstValueKey;
    private final IBufferingIterator<V> firstValue = new BufferingIterator<>();
    private FDate lastValueKey;
    private final IBufferingIterator<V> lastValue = new BufferingIterator<>();

    public RangeTableLiveSegment(final SegmentedKey<K> segmentedKey,
            final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable) {
        this.segmentedKey = segmentedKey;
        this.historicalSegmentTable = historicalSegmentTable;
        this.values = new ADelegateRangeTable<Void, FDate, V>("inProgress") {

            @Override
            protected File getDirectory() {
                return new File(historicalSegmentTable.getDirectory(),
                        historicalSegmentTable.hashKeyToString(segmentedKey));
            }

            @Override
            protected boolean allowHasNext() {
                return true;
            }

            @Override
            protected ISerde<Void> newHashKeySerde() {
                return VoidSerde.GET;
            }

            @Override
            protected ISerde<FDate> newRangeKeySerde() {
                return FDateSerde.GET;
            }

            @Override
            protected ISerde<V> newValueSerde() {
                return historicalSegmentTable.newValueSerde();
            }

        };
    }

    @Override
    public V getFirstValue() {
        return firstValue.getHead();
    }

    @Override
    public V getLastValue() {
        return lastValue.getTail();
    }

    @Override
    public SegmentedKey<K> getSegmentedKey() {
        return segmentedKey;
    }

    @Override
    public ICloseableIterable<V> rangeValues(final FDate from, final FDate to, final Lock readLock,
            final ISkipFileFunction skipFileFunction) {
        //we expect the read lock to be already locked from the outside
        if (values == null || from != null && to != null && from.isAfterNotNullSafe(to)) {
            return EmptyCloseableIterable.getInstance();
        }
        if (from != null && !lastValue.isEmpty() && from.isAfterOrEqualToNotNullSafe(lastValueKey)) {
            if (from.isAfterNotNullSafe(lastValueKey)) {
                return EmptyCloseableIterable.getInstance();
            } else {
                return lastValue.snapshot();
            }
        }
        if (to != null && !firstValue.isEmpty() && to.isBeforeOrEqualToNotNullSafe(firstValueKey)) {
            if (to.isBeforeNotNullSafe(firstValueKey)) {
                return EmptyCloseableIterable.getInstance();
            } else {
                return firstValue.snapshot();
            }
        }
        final ICloseableIterable<V> iterable = new ICloseableIterable<V>() {
            @Override
            public ICloseableIterator<V> iterator() {
                return values.rangeValues(null, from, to);
            }
        };
        if (readLock == DisabledLock.INSTANCE) {
            return iterable;
        } else {
            //we expect the read lock to be already locked from the outside
            return new BufferingIterator<>(iterable);
        }
    }

    @Override
    public ICloseableIterable<V> rangeReverseValues(final FDate from, final FDate to, final Lock readLock,
            final ISkipFileFunction skipFileFunction) {
        //we expect the read lock to be already locked from the outside
        if (values == null || from != null && to != null && from.isBeforeNotNullSafe(to)) {
            return EmptyCloseableIterable.getInstance();
        }
        if (from != null && !firstValue.isEmpty() && from.isBeforeOrEqualToNotNullSafe(firstValueKey)) {
            if (from.isBeforeNotNullSafe(firstValueKey)) {
                return EmptyCloseableIterable.getInstance();
            } else {
                return firstValue.snapshot();
            }
        }
        if (to != null && !lastValue.isEmpty() && to.isAfterOrEqualToNotNullSafe(lastValueKey)) {
            if (to.isAfterNotNullSafe(lastValueKey)) {
                return EmptyCloseableIterable.getInstance();
            } else {
                return lastValue.snapshot();
            }
        }
        final ICloseableIterable<V> iterable = new ICloseableIterable<V>() {
            @Override
            public ICloseableIterator<V> iterator() {
                return values.rangeReverseValues(null, from, to);
            }
        };
        if (readLock == DisabledLock.INSTANCE) {
            return iterable;
        } else {
            //we expect the read lock to be already locked from the outside
            return new BufferingIterator<>(iterable);
        }
    }

    @Override
    public void putNextLiveValue(final FDate nextLiveKey, final V nextLiveValue) {
        if (!lastValue.isEmpty() && lastValueKey.isAfter(nextLiveKey)) {
            LOG.warn("%s: nextLiveKey [%s] should be after or equal to lastLiveKey [%s]", segmentedKey, nextLiveKey,
                    lastValueKey);
            //            throw new IllegalStateException(segmentedKey + ": nextLiveKey [" + nextLiveKey
            //                    + "] should be after or equal to lastLiveKey [" + lastValueKey + "]");
            return;
        }
        values.put(null, nextLiveKey, nextLiveValue);
        if (firstValue.isEmpty() || firstValueKey.equalsNotNullSafe(nextLiveKey)) {
            firstValue.add(nextLiveValue);
            firstValueKey = nextLiveKey;
        }
        if (!lastValue.isEmpty() && !lastValueKey.equalsNotNullSafe(nextLiveKey)) {
            lastValue.clear();
        }
        lastValue.add(nextLiveValue);
        lastValueKey = nextLiveKey;
    }

    @Override
    public V getNextValue(final FDate date, final int shiftForwardUnits) {
        if (!lastValue.isEmpty() && (date == null || date.isAfterOrEqualToNotNullSafe(lastValueKey))) {
            //we always return the last last value
            return lastValue.getTail();
        }
        if (!firstValue.isEmpty() && (date != null && date.isBeforeNotNullSafe(firstValueKey))) {
            //we always return the first first value
            return firstValue.getHead();
        }
        V nextValue = null;
        int shiftForwardRemaining = shiftForwardUnits;
        try (ICloseableIterator<V> rangeValues = rangeValues(date, null, DisabledLock.INSTANCE, null).iterator()) {
            if (shiftForwardUnits == 1) {
                /*
                 * workaround for deteremining next key with multiple values at the same millisecond (without this
                 * workaround we would return a duplicate that might produce an endless loop)
                 */
                while (shiftForwardRemaining >= 0) {
                    final V nextNextValue = rangeValues.next();
                    final FDate nextNextValueKey = historicalSegmentTable.extractEndTime(nextNextValue);
                    if (shiftForwardRemaining == 1 || date.isBeforeNotNullSafe(nextNextValueKey)) {
                        nextValue = nextNextValue;
                        shiftForwardRemaining--;
                    }
                }
            } else {
                while (shiftForwardRemaining >= 0) {
                    final V nextNextValue = rangeValues.next();
                    nextValue = nextNextValue;
                    shiftForwardRemaining--;
                }
            }
        } catch (final NoSuchElementException e) {
            //ignore
        }
        if (nextValue != null) {
            return nextValue;
        } else {
            return lastValue.getTail();
        }
    }

    @Override
    public V getLatestValue(final FDate date) {
        if (!lastValue.isEmpty() && (date == null || date.isAfterOrEqualToNotNullSafe(lastValueKey))) {
            //we always return the last last value
            return lastValue.getTail();
        }
        if (!firstValue.isEmpty() && date != null && date.isBeforeOrEqualToNotNullSafe(firstValueKey)) {
            //we always return the first first value
            return firstValue.getHead();
        }
        return values.getLatestValue(null, date);
    }

    @Override
    public boolean isEmpty() {
        return firstValue.isEmpty();
    }

    @Override
    public void close() {
        values.deleteTable();
        firstValue.clear();
        firstValueKey = null;
        lastValue.clear();
        lastValueKey = null;
    }

    @Override
    public void convertLiveSegmentToHistorical() {
        final ASegmentedTimeSeriesStorageCache<K, V> lookupTableCache = historicalSegmentTable
                .getLookupTableCache(getSegmentedKey().getKey());
        final boolean initialized = lookupTableCache.maybeInitSegment(getSegmentedKey(),
                new Function<SegmentedKey<K>, ICloseableIterable<? extends V>>() {
                    @Override
                    public ICloseableIterable<? extends V> apply(final SegmentedKey<K> t) {
                        return rangeValues(t.getSegment().getFrom(), t.getSegment().getTo(), DisabledLock.INSTANCE,
                                null);
                    }
                });
        if (!initialized) {
            throw new IllegalStateException("true expected");
        }
    }

    @Override
    public FDate getFirstValueKey() {
        return firstValueKey;
    }

    @Override
    public FDate getLastValueKey() {
        return lastValueKey;
    }

}
