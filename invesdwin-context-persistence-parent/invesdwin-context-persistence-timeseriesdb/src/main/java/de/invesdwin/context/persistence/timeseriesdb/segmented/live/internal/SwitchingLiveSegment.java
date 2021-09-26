package de.invesdwin.context.persistence.timeseriesdb.segmented.live.internal;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang3.mutable.MutableInt;

import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseriesdb.segmented.live.ALiveSegmentedTimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.storage.ChunkValue;
import de.invesdwin.context.persistence.timeseriesdb.storage.ISkipFileFunction;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterable;
import de.invesdwin.util.collections.iterable.FlatteningIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import de.invesdwin.util.concurrent.lock.disabled.DisabledLock;
import de.invesdwin.util.concurrent.reference.MutableReference;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class SwitchingLiveSegment<K, V> implements ILiveSegment<K, V> {

    private final SegmentedKey<K> segmentedKey;
    private final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable;
    private final ILiveSegment<K, V> inProgress;
    private final PersistentLiveSegment<K, V> persistent;
    private final List<ILiveSegment<K, V>> latestValueProviders;

    private FDate firstValueKey;
    private final IBufferingIterator<V> firstValue = new BufferingIterator<>();
    private FDate lastValueKey;
    private final IBufferingIterator<V> lastValue = new BufferingIterator<>();
    private int inProgressSize = 0;
    private final int batchFlushInterval;

    public SwitchingLiveSegment(final SegmentedKey<K> segmentedKey,
            final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable,
            final int batchFlushInterval) {
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

    //CHECKSTYLE:OFF
    @Override
    public ICloseableIterable<V> rangeValues(final FDate from, final FDate to, final Lock readLock,
            final ISkipFileFunction skipFileFunction) {
        //CHECKSTYLE:ON
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

    //CHECKSTYLE:OFF
    @Override
    public ICloseableIterable<V> rangeReverseValues(final FDate from, final FDate to, final Lock readLock,
            final ISkipFileFunction skipFileFunction) {
        //CHECKSTYLE:ON
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
    public void putNextLiveValue(final FDate nextLiveKey, final V nextLiveValue) {
        if (!lastValue.isEmpty() && lastValueKey.isAfter(nextLiveKey)) {
            throw new IllegalStateException(segmentedKey + ": nextLiveKey [" + nextLiveKey
                    + "] should be after or equal to lastLiveKey [" + lastValueKey + "]");
        }
        inProgress.putNextLiveValue(nextLiveKey, nextLiveValue);
        if (firstValue.isEmpty() || firstValueKey.equalsNotNullSafe(nextLiveKey)) {
            firstValue.add(nextLiveValue);
            firstValueKey = nextLiveKey;
        }
        if (!lastValue.isEmpty() && !lastValueKey.equalsNotNullSafe(nextLiveKey)) {
            lastValue.clear();
        }
        lastValue.add(nextLiveValue);
        lastValueKey = nextLiveKey;
        inProgressSize++;
        if (inProgressSize >= batchFlushInterval) {
            flushLiveSegment();
        }
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
            final MutableReference<V> nextValue = new MutableReference<>();
            final MutableInt shiftForwardRemaining = new MutableInt(shiftForwardUnits);
            try (ICloseableIterator<V> rangeValues = rangeValues(date, null, DisabledLock.INSTANCE,
                    new ISkipFileFunction() {
                        @Override
                        public boolean skipFile(final ChunkValue file) {
                            final boolean skip = nextValue.get() != null
                                    && file.getValueCount() < shiftForwardRemaining.intValue();
                            if (skip) {
                                shiftForwardRemaining.subtract(file.getValueCount());
                            }
                            return skip;
                        }
                    }).iterator()) {
                while (shiftForwardRemaining.intValue() >= 0) {
                    nextValue.set(rangeValues.next());
                    shiftForwardRemaining.decrement();
                }
            } catch (final NoSuchElementException e) {
                //ignore
            }
            return nextValue.get();
        }
    }

    //CHECKSTYLE:OFF
    @Override
    public V getLatestValue(final FDate date) {
        //CHECKSTYLE:ON
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
            final FDate newValueTime = historicalSegmentTable.extractEndTime(newValue);
            if (newValueTime.isBeforeOrEqualTo(date)) {
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
        persistent.putNextLiveValues(inProgress.rangeValues(inProgress.getFirstValueKey(), inProgress.getLastValueKey(),
                DisabledLock.INSTANCE, null));
        inProgressSize = 0;
        inProgress.close();
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
