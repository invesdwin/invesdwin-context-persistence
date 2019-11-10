package de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.live.internal;

import java.io.File;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ezdb.ADelegateRangeTable;
import de.invesdwin.context.persistence.timeseries.serde.FDateSerde;
import de.invesdwin.context.persistence.timeseries.serde.VoidSerde;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.ASegmentedTimeSeriesStorageCache;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.live.ALiveSegmentedTimeSeriesDB;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.concurrent.lock.disabled.DisabledLock;
import de.invesdwin.util.time.fdate.FDate;
import ezdb.serde.Serde;

@NotThreadSafe
public class RangeTableLiveSegment<K, V> implements ILiveSegment<K, V> {

    private final SegmentedKey<K> segmentedKey;
    private final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable;
    private final ADelegateRangeTable<Void, FDate, V> values;
    private FDate firstValueKey;
    private V firstValue;
    private FDate lastValueKey;
    private V lastValue;

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
            protected Serde<Void> newHashKeySerde() {
                return VoidSerde.GET;
            }

            @Override
            protected Serde<FDate> newRangeKeySerde() {
                return FDateSerde.GET;
            }

            @Override
            protected Serde<V> newValueSerde() {
                return historicalSegmentTable.newValueSerde();
            }

        };
    }

    @Override
    public V getFirstValue() {
        return firstValue;
    }

    @Override
    public V getLastValue() {
        return lastValue;
    }

    @Override
    public SegmentedKey<K> getSegmentedKey() {
        return segmentedKey;
    }

    @Override
    public ICloseableIterable<V> rangeValues(final FDate from, final FDate to, final Lock readLock) {
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
    public ICloseableIterable<V> rangeReverseValues(final FDate from, final FDate to, final Lock readLock) {
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
        values.put(null, nextLiveKey, nextLiveValue);
        if (firstValue == null) {
            firstValue = nextLiveValue;
            firstValueKey = nextLiveKey;
        }
        lastValue = nextLiveValue;
        lastValueKey = nextLiveKey;
    }

    @Override
    public V getNextValue(final FDate date, final int shiftForwardUnits) {
        if (lastValue != null && (date == null || date.isAfterOrEqualToNotNullSafe(lastValueKey))) {
            return lastValue;
        }
        if (firstValue != null && (date != null && date.isBeforeNotNullSafe(firstValueKey))) {
            return firstValue;
        }
        V nextValue = null;
        try (ICloseableIterator<V> rangeValues = rangeValues(date, null, DisabledLock.INSTANCE).iterator()) {
            for (int i = 0; i < shiftForwardUnits; i++) {
                nextValue = rangeValues.next();
            }
        } catch (final NoSuchElementException e) {
            //ignore
        }
        if (nextValue != null) {
            return nextValue;
        } else {
            return lastValue;
        }
    }

    @Override
    public V getLatestValue(final FDate date) {
        if (lastValue != null && (date == null || date.isAfterOrEqualToNotNullSafe(lastValueKey))) {
            return lastValue;
        }
        if (firstValue != null && (date != null && date.isBeforeOrEqualToNotNullSafe(firstValueKey))) {
            return firstValue;
        }
        return values.getLatestValue(null, date);
    }

    @Override
    public boolean isEmpty() {
        return firstValue == null;
    }

    @Override
    public void close() {
        values.deleteTable();
        firstValue = null;
        firstValueKey = null;
        lastValue = null;
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
                        return rangeValues(t.getSegment().getFrom(), t.getSegment().getTo(), DisabledLock.INSTANCE);
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
