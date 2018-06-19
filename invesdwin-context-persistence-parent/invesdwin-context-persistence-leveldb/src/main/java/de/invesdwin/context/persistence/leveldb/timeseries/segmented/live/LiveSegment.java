package de.invesdwin.context.persistence.leveldb.timeseries.segmented.live;

import java.io.Closeable;
import java.io.File;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.leveldb.ezdb.ADelegateRangeTable;
import de.invesdwin.context.persistence.leveldb.serde.FDateSerde;
import de.invesdwin.context.persistence.leveldb.serde.VoidSerde;
import de.invesdwin.context.persistence.leveldb.timeseries.segmented.SegmentedKey;
import de.invesdwin.util.collections.iterable.ATransformingCloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.time.fdate.FDate;
import ezdb.TableRow;
import ezdb.serde.Serde;

@NotThreadSafe
public class LiveSegment<K, V> implements Closeable {

    //CHECKSTYLE:OFF
    private final ADelegateRangeTable<Void, FDate, V> values;
    //CHECKSTYLE:ON
    private FDate lastValueKey;
    private V lastValue;
    private final SegmentedKey<K> segmentedKey;

    public LiveSegment(final SegmentedKey<K> segmentedKey,
            final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable) {
        this.segmentedKey = segmentedKey;
        this.values = new ADelegateRangeTable<Void, FDate, V>(LiveSegment.class.getSimpleName()) {

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

            @Override
            protected File getBaseDirectory() {
                return historicalSegmentTable.getStorage()
                        .newDataDirectory(historicalSegmentTable.hashKeyToString(segmentedKey.getKey()));
            }

            @Override
            protected boolean allowPutWithoutBatch() {
                return true;
            }

            @Override
            protected boolean allowHasNext() {
                return true;
            }

            @Override
            protected File getDirectory() {
                return new File(getBaseDirectory(), LiveSegment.class.getSimpleName());
            }

        };
    }

    public V getFirstValue() {
        final TableRow<Void, FDate, V> firstEntry = values.getLatest(null, FDate.MIN_DATE);
        if (firstEntry != null) {
            return firstEntry.getValue();
        } else {
            return null;
        }
    }

    public V getLastValue() {
        return lastValue;
    }

    public SegmentedKey<K> getSegmentedKey() {
        return segmentedKey;
    }

    public ICloseableIterable<V> rangeValues(final FDate from, final FDate to) {
        final ICloseableIterable<TableRow<Void, FDate, V>> tail = new ICloseableIterable<TableRow<Void, FDate, V>>() {
            @Override
            public ICloseableIterator<TableRow<Void, FDate, V>> iterator() {
                return values.range(null, from, to);
            }
        };
        return new ATransformingCloseableIterable<TableRow<Void, FDate, V>, V>(tail) {
            @Override
            protected V transform(final TableRow<Void, FDate, V> value) {
                return value.getValue();
            }
        };
    }

    public ICloseableIterable<V> rangeReverseValues(final FDate from, final FDate to) {
        final ICloseableIterable<TableRow<Void, FDate, V>> tail = new ICloseableIterable<TableRow<Void, FDate, V>>() {
            @Override
            public ICloseableIterator<TableRow<Void, FDate, V>> iterator() {
                return values.rangeReverse(null, from, to);
            }
        };
        return new ATransformingCloseableIterable<TableRow<Void, FDate, V>, V>(tail) {
            @Override
            protected V transform(final TableRow<Void, FDate, V> value) {
                return value.getValue();
            }
        };
    }

    public void putNextLiveValue(final FDate nextLiveKey, final V nextLiveValue) {
        if (lastValue != null && lastValueKey.isAfterOrEqualTo(nextLiveKey)) {
            throw new IllegalStateException(segmentedKey + ": nextLiveKey [" + nextLiveKey
                    + "] should be after lastLiveKey [" + lastValueKey + "]");
        }
        values.put(null, nextLiveKey, nextLiveValue);
        lastValue = nextLiveValue;
        lastValueKey = nextLiveKey;
    }

    public V getNextValue(final FDate date, final int shiftForwardUnits) {
        V nextValue = null;
        try (ICloseableIterator<V> rangeValues = rangeValues(date, null).iterator()) {
            for (int i = 0; i < shiftForwardUnits; i++) {
                nextValue = rangeValues.next();
            }
        } catch (final NoSuchElementException e) {
            //ignore
        }
        if (nextValue != null) {
            return nextValue;
        } else {
            return getLastValue();
        }
    }

    public V getLatestValue(final FDate date) {
        final TableRow<Void, FDate, V> floorEntry = values.getLatest(null, date);
        if (floorEntry != null) {
            return floorEntry.getValue();
        } else {
            return getFirstValue();
        }
    }

    public boolean isEmpty() {
        return lastValue == null;
    }

    @Override
    public void close() {
        values.deleteTable();
    }

}
