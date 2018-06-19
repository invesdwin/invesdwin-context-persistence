package de.invesdwin.context.persistence.leveldb.timeseries.segmented.live.internal;

import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.leveldb.timeseries.segmented.ASegmentedTimeSeriesStorageCache;
import de.invesdwin.context.persistence.leveldb.timeseries.segmented.SegmentedKey;
import de.invesdwin.context.persistence.leveldb.timeseries.segmented.live.ALiveSegmentedTimeSeriesDB;
import de.invesdwin.util.collections.iterable.ASkippingIterable;
import de.invesdwin.util.collections.iterable.ATransformingCloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.time.fdate.FDate;
import uk.co.omegaprime.btreemap.LongObjectBTreeMap;

@NotThreadSafe
public class PersistentLiveSegment<K, V> implements ILiveSegment<K, V> {

    //CHECKSTYLE:OFF
    private final LongObjectBTreeMap<V> values = LongObjectBTreeMap.create();
    //CHECKSTYLE:ON
    private FDate lastValueKey;
    private V lastValue;
    private final SegmentedKey<K> segmentedKey;
    private final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable;

    public PersistentLiveSegment(final SegmentedKey<K> segmentedKey,
            final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable) {
        this.segmentedKey = segmentedKey;
        this.historicalSegmentTable = historicalSegmentTable;
    }

    @Override
    public V getFirstValue() {
        final Entry<Long, V> firstEntry = values.firstEntry();
        if (firstEntry != null) {
            return firstEntry.getValue();
        } else {
            return null;
        }
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
    public ICloseableIterable<V> rangeValues(final FDate from, final FDate to) {
        final SortedMap<Long, V> tailMap;
        if (from == null) {
            tailMap = values;
        } else {
            tailMap = values.tailMap(from.millisValue());
        }
        final ICloseableIterable<Entry<Long, V>> tail = WrapperCloseableIterable.maybeWrap(tailMap.entrySet());
        final ICloseableIterable<Entry<Long, V>> skipping;
        if (to == null) {
            skipping = tail;
        } else {
            skipping = new ASkippingIterable<Entry<Long, V>>(tail) {
                @Override
                protected boolean skip(final Entry<Long, V> element) {
                    if (element.getKey() > to.millisValue()) {
                        throw new FastNoSuchElementException("LiveSegment rangeValues end reached");
                    }
                    return false;
                }
            };
        }
        return new ATransformingCloseableIterable<Entry<Long, V>, V>(skipping) {
            @Override
            protected V transform(final Entry<Long, V> value) {
                return value.getValue();
            }
        };
    }

    @Override
    public ICloseableIterable<V> rangeReverseValues(final FDate from, final FDate to) {
        final SortedMap<Long, V> headMap;
        if (from == null) {
            headMap = values.descendingMap();
        } else {
            headMap = values.descendingMap().tailMap(from.millisValue());
        }
        final ICloseableIterable<Entry<Long, V>> tail = WrapperCloseableIterable.maybeWrap(headMap.entrySet());
        final ICloseableIterable<Entry<Long, V>> skipping;
        if (to == null) {
            skipping = tail;
        } else {
            skipping = new ASkippingIterable<Entry<Long, V>>(tail) {
                @Override
                protected boolean skip(final Entry<Long, V> element) {
                    if (element.getKey() > to.millisValue()) {
                        throw new FastNoSuchElementException("LiveSegment rangeReverseValues end reached");
                    }
                    return false;
                }
            };
        }
        return new ATransformingCloseableIterable<Entry<Long, V>, V>(skipping) {
            @Override
            protected V transform(final Entry<Long, V> value) {
                return value.getValue();
            }
        };
    }

    @Override
    public void putNextLiveValue(final FDate nextLiveKey, final V nextLiveValue) {
        if (lastValue != null && lastValueKey.isAfterOrEqualTo(nextLiveKey)) {
            throw new IllegalStateException(segmentedKey + ": nextLiveKey [" + nextLiveKey
                    + "] should be after lastLiveKey [" + lastValueKey + "]");
        }
        values.put(nextLiveKey.millisValue(), nextLiveValue);
        lastValue = nextLiveValue;
        lastValueKey = nextLiveKey;
    }

    @Override
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

    @Override
    public V getLatestValue(final FDate date) {
        final Entry<Long, V> floorEntry = values.floorEntry(date.millisValue());
        if (floorEntry != null) {
            return floorEntry.getValue();
        } else {
            return getFirstValue();
        }
    }

    @Override
    public boolean isEmpty() {
        return values.isEmpty();
    }

    @Override
    public void close() {}

    @Override
    public void convertLiveSegmentToHistorical() {
        final ASegmentedTimeSeriesStorageCache<K, V> lookupTableCache = historicalSegmentTable
                .getLookupTableCache(getSegmentedKey().getKey());
        final boolean initialized = lookupTableCache.maybeInitSegment(getSegmentedKey(),
                new Function<SegmentedKey<K>, ICloseableIterable<? extends V>>() {
                    @Override
                    public ICloseableIterable<? extends V> apply(final SegmentedKey<K> t) {
                        return rangeValues(t.getSegment().getFrom(), t.getSegment().getTo());
                    }
                });
        if (!initialized) {
            throw new IllegalStateException("true expected");
        }
    }

}
