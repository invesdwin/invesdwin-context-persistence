package de.invesdwin.context.persistence.timeseriesdb.segmented.live.segment;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.timeseriesdb.loop.ShiftForwardUnitsLoop;
import de.invesdwin.context.persistence.timeseriesdb.segmented.ISegmentedTimeSeriesDBInternals;
import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseriesdb.storage.ISkipFileFunction;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterable;
import de.invesdwin.util.collections.iterable.FlatteningIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.disabled.DisabledLock;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class SwitchingLiveSegment<K, V> implements ILiveSegment<K, V> {

    private static final Log LOG = new Log(SwitchingLiveSegment.class);
    private final SegmentedKey<K> segmentedKey;
    private final ISegmentedTimeSeriesDBInternals<K, V> historicalSegmentTable;
    private final ILiveSegment<K, V> inProgress;
    private final PersistentLiveSegment<K, V> persistent;
    private final List<ILiveSegment<K, V>> latestValueProviders;

    private FDate firstValueKey;
    private final IBufferingIterator<V> firstValue = new BufferingIterator<>();
    private volatile FDate lastValueKey;
    private final IBufferingIterator<V> lastValue = new BufferingIterator<>();
    private int inProgressSize = 0;
    private final int batchFlushInterval;

    @SuppressWarnings("unchecked")
    public SwitchingLiveSegment(final SegmentedKey<K> segmentedKey,
            final ISegmentedTimeSeriesDBInternals<K, V> historicalSegmentTable, final int batchFlushInterval) {
        this.segmentedKey = segmentedKey;
        this.historicalSegmentTable = historicalSegmentTable;
        this.inProgress = new FileLiveSegment<>(segmentedKey, historicalSegmentTable);
        this.persistent = new PersistentLiveSegment<>(segmentedKey, historicalSegmentTable);
        this.latestValueProviders = Arrays.asList(inProgress, persistent);
        this.batchFlushInterval = batchFlushInterval;
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
        if (isEmpty() || from != null && to != null && from.isAfterNotNullSafe(to)) {
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
        if (inProgress.isEmpty()) {
            //no live segment, go with historical
            return persistent.rangeValues(from, to, readLock, skipFileFunction);
        } else if (persistent.isEmpty()) {
            return inProgress.rangeValues(from, to, readLock, skipFileFunction);
        } else {
            final FDate memoryFrom = inProgress.getFirstValueKey();
            if (memoryFrom.isAfter(to)) {
                //live segment is after requested range, go with historical
                return persistent.rangeValues(from, to, readLock, skipFileFunction);
            } else if (memoryFrom.isBeforeOrEqualTo(from)) {
                //historical segment is before requested range, go with live
                return inProgress.rangeValues(from, to, readLock, skipFileFunction);
            } else {
                //use both segments
                final ICloseableIterable<V> historicalRangeValues = persistent.rangeValues(from, memoryFrom, readLock,
                        skipFileFunction);
                final ICloseableIterable<V> liveRangeValues = inProgress.rangeValues(memoryFrom, to, readLock,
                        skipFileFunction);
                return new FlatteningIterable<V>(historicalRangeValues, liveRangeValues);
            }
        }
    }

    @Override
    public ICloseableIterable<V> rangeReverseValues(final FDate from, final FDate to, final ILock readLock,
            final ISkipFileFunction skipFileFunction) {
        //we expect the read lock to be already locked from the outside
        if (isEmpty() || from != null && to != null && from.isBeforeNotNullSafe(to)) {
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
        if (inProgress.isEmpty()) {
            //no live segment, go with historical
            return persistent.rangeReverseValues(from, to, readLock, skipFileFunction);
        } else if (persistent.isEmpty()) {
            return inProgress.rangeReverseValues(from, to, readLock, skipFileFunction);
        } else {
            final FDate memoryFrom = inProgress.getFirstValueKey();
            if (memoryFrom.isAfter(from)) {
                //live segment is after requested range, go with historical
                return persistent.rangeReverseValues(from, to, readLock, skipFileFunction);
            } else if (memoryFrom.isBeforeOrEqualTo(to)) {
                //historical segment is before requested range, go with live
                return inProgress.rangeReverseValues(from, to, readLock, skipFileFunction);
            } else {
                //use both segments
                final ICloseableIterable<V> liveRangeValues = inProgress.rangeReverseValues(from, memoryFrom, readLock,
                        skipFileFunction);
                final ICloseableIterable<V> historicalRangeValues = persistent.rangeReverseValues(memoryFrom, to,
                        readLock, skipFileFunction);
                return new FlatteningIterable<V>(liveRangeValues, historicalRangeValues);
            }
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
        inProgress.putNextLiveValue(nextLiveStartTime, nextLiveEndTimeKey, nextLiveValue);
        if (firstValue.isEmpty() || firstValueKey.equalsNotNullSafe(nextLiveEndTimeKey)) {
            firstValue.add(nextLiveValue);
            firstValueKey = nextLiveEndTimeKey;
        }
        if (!lastValue.isEmpty() && !lastValueKey.equalsNotNullSafe(nextLiveEndTimeKey)) {
            lastValue.clear();
        }
        lastValue.add(nextLiveValue);
        lastValueKey = nextLiveEndTimeKey;
        inProgressSize++;
        if (inProgressSize >= batchFlushInterval) {
            flushLiveSegment();
        }
        return true;
    }

    @Override
    public long size() {
        return inProgress.size() + persistent.size();
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
        if (inProgress.isEmpty()) {
            //no live segment, go with historical
            return persistent.getNextValue(date, shiftForwardUnits);
        } else if (persistent.isEmpty() || inProgress.getFirstValueKey().isBefore(date)) {
            //live segment is after requested range, go with live
            final V nextValue = inProgress.getNextValue(date, shiftForwardUnits);
            return nextValue;
        } else {
            //use both segments
            final ShiftForwardUnitsLoop<V> shiftForwardLoop = new ShiftForwardUnitsLoop<>(date, shiftForwardUnits,
                    historicalSegmentTable::extractEndTime);
            final ICloseableIterable<V> rangeValues = rangeValues(date, null, DisabledLock.INSTANCE, file -> {
                final boolean skip = shiftForwardLoop.getNextValue() != null
                        && file.getValueCount() < shiftForwardLoop.getShiftForwardRemaining();
                if (skip) {
                    shiftForwardLoop.skip(file.getValueCount());
                }
                return skip;
            });
            shiftForwardLoop.loop(rangeValues);
            return shiftForwardLoop.getNextValue();
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
        if (inProgress.isEmpty()) {
            return persistent.getLatestValue(date);
        } else if (persistent.isEmpty()) {
            return inProgress.getLatestValue(date);
        }
        V latestValue = null;
        for (int i = 0; i < latestValueProviders.size(); i++) {
            final ILiveSegment<K, V> latestValueProvider = latestValueProviders.get(i);
            final V newValue = latestValueProvider.getLatestValue(date);
            final FDate newValueEndTime = historicalSegmentTable.extractEndTime(newValue);
            if (newValueEndTime.isBeforeOrEqualTo(date)) {
                /*
                 * even if we got the first value in this segment and it is after the desired key we just continue to
                 * the beginning to search for an earlier value until we reach the overall firstValue
                 */
                latestValue = newValue;
                break;
            }
        }
        if (latestValue == null) {
            latestValue = getFirstValue();
        }
        return latestValue;
    }

    @Override
    public V getLatestValue(final long index) {
        if (!lastValue.isEmpty() && index >= size() - 1) {
            //we always return the last last value
            return lastValue.getTail();
        }
        if (!firstValue.isEmpty() && index <= 0) {
            //we always return the first first value
            return firstValue.getHead();
        }
        if (inProgress.isEmpty()) {
            return persistent.getLatestValue(index);
        } else if (persistent.isEmpty()) {
            final long inProgressIndex = index - persistent.size();
            return inProgress.getLatestValue(inProgressIndex);
        }
        V latestValue = null;
        long precedingValueCount = 0L;
        for (int i = latestValueProviders.size() - 1; i >= 0; i--) {
            final ILiveSegment<K, V> latestValueProvider = latestValueProviders.get(i);
            final long combinedValueCount = precedingValueCount + latestValueProvider.size();
            if (precedingValueCount <= index && index < combinedValueCount) {
                final V newValue = latestValueProvider.getLatestValue(index - precedingValueCount);
                /*
                 * even if we got the first value in this segment and it is after the desired key we just continue to
                 * the beginning to search for an earlier value until we reach the overall firstValue
                 */
                latestValue = newValue;
                break;
            }
            precedingValueCount = combinedValueCount;
        }
        if (latestValue == null) {
            latestValue = getFirstValue();
        }
        return latestValue;
    }

    @Override
    public long getLatestValueIndex(final FDate date) {
        if (!lastValue.isEmpty() && (date == null || date.isAfterOrEqualToNotNullSafe(lastValueKey))) {
            //we always return the last last value
            return size();
        }
        if (!firstValue.isEmpty() && date != null && date.isBeforeOrEqualToNotNullSafe(firstValueKey)) {
            //we always return the first first value
            return 0L;
        }
        if (inProgress.isEmpty()) {
            return persistent.getLatestValueIndex(date);
        } else if (persistent.isEmpty()) {
            return inProgress.getLatestValueIndex(date) + persistent.size();
        }
        long latestValueIndex = -1L;
        long precedingValueCount = 0L;
        for (int i = latestValueProviders.size() - 1; i >= 0; i--) {
            final ILiveSegment<K, V> latestValueProvider = latestValueProviders.get(i);
            final long newValueIndex = latestValueProvider.getLatestValueIndex(date);
            final V newValue = latestValueProvider.getLatestValue(latestValueIndex);
            final FDate newValueTime = historicalSegmentTable.extractEndTime(newValue);
            if (newValueTime.isBeforeOrEqualTo(date)) {
                /*
                 * even if we got the first value in this segment and it is after the desired key we just continue to
                 * the beginning to search for an earlier value until we reach the overall firstValue
                 */
                latestValueIndex = newValueIndex + precedingValueCount;
                break;
            }
            precedingValueCount += latestValueProvider.size();
        }
        if (latestValueIndex == -1L && getFirstValue() != null) {
            return 0L;
        }
        return latestValueIndex;
    }

    @Override
    public boolean isEmpty() {
        return firstValue.isEmpty();
    }

    @Override
    public void close() {
        firstValue.clear();
        firstValueKey = null;
        lastValue.clear();
        lastValueKey = null;
        inProgress.close();
        persistent.close();
    }

    @Override
    public void convertLiveSegmentToHistorical() {
        if (!inProgress.isEmpty()) {
            flushLiveSegment();
        }
        persistent.finish();
    }

    private void flushLiveSegment() {
        final ICloseableIterable<V> flushedValues = inProgress.rangeValues(inProgress.getFirstValueKey(),
                inProgress.getLastValueKey(), DisabledLock.INSTANCE, null);
        persistent.putNextLiveValues(flushedValues);
        onFlushLiveSegment(flushedValues);
        inProgressSize = 0;
        inProgress.close();
    }

    protected void onFlushLiveSegment(final ICloseableIterable<V> flushedValues) {}

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
            final T unwrapped = inProgress.unwrap(type);
            if (unwrapped != null) {
                return unwrapped;
            }
            return persistent.unwrap(type);
        }
    }

}
