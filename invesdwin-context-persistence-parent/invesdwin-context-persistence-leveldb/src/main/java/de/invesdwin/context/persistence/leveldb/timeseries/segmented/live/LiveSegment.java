package de.invesdwin.context.persistence.leveldb.timeseries.segmented.live;

import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.leveldb.timeseries.segmented.SegmentedKey;
import de.invesdwin.util.collections.iterable.ASkippingIterable;
import de.invesdwin.util.collections.iterable.ATransformingCloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.time.fdate.FDate;

@NotThreadSafe
public class LiveSegment<K, V> {

    //CHECKSTYLE:OFF
    private final TreeMap<FDate, V> values = new TreeMap<FDate, V>(FDate.COMPARATOR);
    //CHECKSTYLE:ON
    private FDate lastValueKey;
    private V lastValue;
    private final SegmentedKey<K> segmentedKey;

    public LiveSegment(final SegmentedKey<K> segmentedKey) {
        this.segmentedKey = segmentedKey;
    }

    public V getFirstValue() {
        final Entry<FDate, V> firstEntry = values.firstEntry();
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
        final SortedMap<FDate, V> tailMap;
        if (from == null) {
            tailMap = values;
        } else {
            tailMap = values.tailMap(from);
        }
        final ICloseableIterable<Entry<FDate, V>> tail = WrapperCloseableIterable.maybeWrap(tailMap.entrySet());
        final ASkippingIterable<Entry<FDate, V>> skipping = new ASkippingIterable<Entry<FDate, V>>(tail) {
            @Override
            protected boolean skip(final Entry<FDate, V> element) {
                if (element.getKey().isAfter(to)) {
                    throw new FastNoSuchElementException("LiveSegment rangeValues end reached");
                }
                return false;
            }
        };
        return new ATransformingCloseableIterable<Entry<FDate, V>, V>(skipping) {
            @Override
            protected V transform(final Entry<FDate, V> value) {
                return value.getValue();
            }
        };
    }

    public ICloseableIterable<V> rangeReverseValues(final FDate from, final FDate to) {
        final SortedMap<FDate, V> headMap;
        if (from == null) {
            headMap = values.descendingMap();
        } else {
            headMap = values.descendingMap().tailMap(from);
        }
        final ICloseableIterable<Entry<FDate, V>> tail = WrapperCloseableIterable.maybeWrap(headMap.entrySet());
        final ASkippingIterable<Entry<FDate, V>> skipping = new ASkippingIterable<Entry<FDate, V>>(tail) {
            @Override
            protected boolean skip(final Entry<FDate, V> element) {
                if (element.getKey().isAfter(to)) {
                    throw new FastNoSuchElementException("LiveSegment rangeValues end reached");
                }
                return false;
            }
        };
        return new ATransformingCloseableIterable<Entry<FDate, V>, V>(skipping) {
            @Override
            protected V transform(final Entry<FDate, V> value) {
                return value.getValue();
            }
        };
    }

    public void putNextLiveValue(final FDate nextLiveKey, final V nextLiveValue) {
        if (lastValue != null && lastValueKey.isAfterOrEqualTo(nextLiveKey)) {
            throw new IllegalStateException(segmentedKey + ": nextLiveKey [" + nextLiveKey
                    + "] should be after lastLiveKey [" + lastValueKey + "]");
        }
        values.put(nextLiveKey, nextLiveValue);
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
        final Entry<FDate, V> floorEntry = values.floorEntry(date);
        if (floorEntry != null) {
            return floorEntry.getValue();
        } else {
            return getFirstValue();
        }
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

}
