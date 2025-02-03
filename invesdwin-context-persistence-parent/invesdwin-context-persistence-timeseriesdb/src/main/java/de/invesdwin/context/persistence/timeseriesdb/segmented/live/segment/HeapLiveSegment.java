package de.invesdwin.context.persistence.timeseriesdb.segmented.live.segment;

import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.timeseriesdb.loop.ShiftForwardUnitsLoop;
import de.invesdwin.context.persistence.timeseriesdb.segmented.ASegmentedTimeSeriesStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.segmented.ISegmentedTimeSeriesDBInternals;
import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseriesdb.storage.ISkipFileFunction;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.iterable.ATransformingIterable;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import de.invesdwin.util.collections.iterable.skip.ASkippingIterable;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.disabled.DisabledLock;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class HeapLiveSegment<K, V> implements ILiveSegment<K, V> {

    private static final Log LOG = new Log(HeapLiveSegment.class);
    private final NavigableMap<Long, V> values = ILockCollectionFactory.getInstance(false).newTreeMap();
    private final SegmentedKey<K> segmentedKey;
    private final ISegmentedTimeSeriesDBInternals<K, V> historicalSegmentTable;
    private FDate firstValueKey;
    private final IBufferingIterator<V> firstValue = new BufferingIterator<>();
    private FDate lastValueKey;
    private final IBufferingIterator<V> lastValue = new BufferingIterator<>();

    public HeapLiveSegment(final SegmentedKey<K> segmentedKey,
            final ISegmentedTimeSeriesDBInternals<K, V> historicalSegmentTable) {
        this.segmentedKey = segmentedKey;
        this.historicalSegmentTable = historicalSegmentTable;
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
    public ICloseableIterable<V> rangeValues(final FDate from, final FDate to, final ILock readLock,
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

        final SortedMap<Long, V> tailMap;
        if (from == null) {
            tailMap = values;
        } else {
            tailMap = values.tailMap(from.millisValue(), true);
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
                        throw FastNoSuchElementException.getInstance("LiveSegment rangeValues end reached");
                    }
                    return false;
                }
            };
        }
        final ATransformingIterable<Entry<Long, V>, V> transforming = new ATransformingIterable<Entry<Long, V>, V>(
                skipping) {
            @Override
            protected V transform(final Entry<Long, V> value) {
                return value.getValue();
            }
        };
        if (readLock == DisabledLock.INSTANCE) {
            return transforming;
        } else {
            //we expect the read lock to be already locked from the outside
            return new BufferingIterator<>(transforming);
        }
    }

    @Override
    public ICloseableIterable<V> rangeReverseValues(final FDate from, final FDate to, final ILock readLock,
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
        final SortedMap<Long, V> headMap;
        if (from == null) {
            headMap = values.descendingMap();
        } else {
            headMap = values.descendingMap().tailMap(from.millisValue(), true);
        }
        final ICloseableIterable<Entry<Long, V>> tail = WrapperCloseableIterable.maybeWrap(headMap.entrySet());
        final ICloseableIterable<Entry<Long, V>> skipping;
        if (to == null) {
            skipping = tail;
        } else {
            skipping = new ASkippingIterable<Entry<Long, V>>(tail) {
                @Override
                protected boolean skip(final Entry<Long, V> element) {
                    if (element.getKey() < to.millisValue()) {
                        throw FastNoSuchElementException.getInstance("LiveSegment rangeReverseValues end reached");
                    }
                    return false;
                }
            };
        }

        final ATransformingIterable<Entry<Long, V>, V> transforming = new ATransformingIterable<Entry<Long, V>, V>(
                skipping) {
            @Override
            protected V transform(final Entry<Long, V> value) {
                return value.getValue();
            }
        };
        if (readLock == DisabledLock.INSTANCE) {
            return transforming;
        } else {
            //we expect the read lock to be already locked from the outside
            return new BufferingIterator<>(transforming);
        }
    }

    @Override
    public boolean putNextLiveValue(final FDate nextLiveStartTime, final FDate nextLiveEndTimeKey,
            final V nextLiveValue) {
        if (!lastValue.isEmpty()) {
            if (lastValueKey.isAfterNotNullSafe(nextLiveStartTime)) {
                LOG.warn("%s: nextLiveStartTime [%s] should be after or equal to lastLiveKey [%s]", segmentedKey,
                        nextLiveStartTime, lastValueKey);
                return false;
            }
        }
        if (nextLiveStartTime.isAfterNotNullSafe(nextLiveEndTimeKey)) {
            throw new IllegalArgumentException(TextDescription.format(
                    "%s: nextLiveEndTimeKey [%s] should be after or equal to nextLiveStartTime [%s]", segmentedKey,
                    nextLiveEndTimeKey, nextLiveStartTime));
        }
        values.put(nextLiveEndTimeKey.millisValue(), nextLiveValue);
        if (firstValue.isEmpty() || firstValueKey.equalsNotNullSafe(nextLiveEndTimeKey)) {
            firstValue.add(nextLiveValue);
            firstValueKey = nextLiveEndTimeKey;
        }
        if (!lastValue.isEmpty() && !lastValueKey.equalsNotNullSafe(nextLiveEndTimeKey)) {
            lastValue.clear();
        }
        lastValue.add(nextLiveValue);
        lastValueKey = nextLiveEndTimeKey;
        return true;
    }

    @Override
    public long size() {
        return values.size();
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
        final ShiftForwardUnitsLoop<V> shiftForwardLoop = new ShiftForwardUnitsLoop<>(date, shiftForwardUnits,
                historicalSegmentTable::extractEndTime);
        final ICloseableIterable<V> rangeValues = rangeValues(date, null, DisabledLock.INSTANCE, null);
        shiftForwardLoop.loop(rangeValues);
        if (shiftForwardLoop.getNextValue() != null) {
            return shiftForwardLoop.getNextValue();
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
        final Entry<Long, V> floorEntry = values.floorEntry(date.millisValue());
        if (floorEntry != null) {
            return floorEntry.getValue();
        } else {
            return getFirstValue();
        }
    }

    @Override
    public V getLatestValue(final long index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLatestValueIndex(final FDate date) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        return values.isEmpty();
    }

    @Override
    public void close() {
        values.clear();
        firstValue.clear();
        firstValueKey = null;
        lastValue.clear();
        lastValueKey = null;
    }

    @Override
    public void convertLiveSegmentToHistorical() {
        final ASegmentedTimeSeriesStorageCache<K, V> lookupTableCache = historicalSegmentTable
                .getSegmentedLookupTableCache(getSegmentedKey().getKey());
        final boolean initialized = lookupTableCache.maybeInitSegmentSync(getSegmentedKey(),
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

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(final Class<T> type) {
        if (type.isAssignableFrom(getClass())) {
            return (T) this;
        } else {
            return null;
        }
    }

}
