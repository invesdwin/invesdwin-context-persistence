package de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.live.internal;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.live.ALiveSegmentedTimeSeriesDB;
import de.invesdwin.util.collections.iterable.FlatteningIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.time.fdate.FDate;

@NotThreadSafe
public class SwitchingLiveSegment<K, V> implements ILiveSegment<K, V> {

    private final SegmentedKey<K> segmentedKey;
    private final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable;
    private final ILiveSegment<K, V> inProgress;
    private final PersistentLiveSegment<K, V> persistent;
    private final List<ILiveSegment<K, V>> latestValueProviders;

    private FDate firstValueKey;
    private V firstValue;
    private FDate lastValueKey;
    private V lastValue;
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
    public FDate getFirstValueKey() {
        return firstValueKey;
    }

    @Override
    public V getFirstValue() {
        return firstValue;
    }

    @Override
    public FDate getLastValueKey() {
        return lastValueKey;
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
        if (inProgress.isEmpty()) {
            //no live segment, go with historical
            return persistent.rangeValues(from, to);
        } else if (persistent.isEmpty()) {
            return inProgress.rangeValues(from, to);
        } else {
            final FDate memoryFrom = inProgress.getFirstValueKey();
            if (memoryFrom.isAfter(to)) {
                //live segment is after requested range, go with historical
                return persistent.rangeValues(from, to);
            } else if (memoryFrom.isBeforeOrEqualTo(from)) {
                //historical segment is before requested range, go with live
                return inProgress.rangeValues(from, to);
            } else {
                //use both segments
                final ICloseableIterable<V> historicalRangeValues = persistent.rangeValues(from,
                        memoryFrom.addMilliseconds(-1));
                final ICloseableIterable<V> liveRangeValues = inProgress.rangeValues(memoryFrom, to);
                return new FlatteningIterable<V>(historicalRangeValues, liveRangeValues);
            }
        }
    }

    @Override
    public ICloseableIterable<V> rangeReverseValues(final FDate from, final FDate to) {
        if (inProgress.isEmpty()) {
            //no live segment, go with historical
            return persistent.rangeReverseValues(from, to);
        } else if (persistent.isEmpty()) {
            return inProgress.rangeReverseValues(from, to);
        } else {
            final FDate memoryFrom = inProgress.getFirstValueKey();
            if (memoryFrom.isAfter(from)) {
                //live segment is after requested range, go with historical
                return persistent.rangeReverseValues(from, to);
            } else if (memoryFrom.isBeforeOrEqualTo(to)) {
                //historical segment is before requested range, go with live
                return inProgress.rangeReverseValues(from, to);
            } else {
                //use both segments
                final ICloseableIterable<V> liveRangeValues = inProgress.rangeReverseValues(from, memoryFrom);
                final ICloseableIterable<V> historicalRangeValues = persistent
                        .rangeReverseValues(memoryFrom.addMilliseconds(-1), to);
                return new FlatteningIterable<V>(liveRangeValues, historicalRangeValues);
            }
        }
    }

    @Override
    public void putNextLiveValue(final FDate nextLiveKey, final V nextLiveValue) {
        if (lastValue != null && lastValueKey.isAfterOrEqualTo(nextLiveKey)) {
            throw new IllegalStateException(segmentedKey + ": nextLiveKey [" + nextLiveKey
                    + "] should be after lastLiveKey [" + lastValueKey + "]");
        }
        inProgress.putNextLiveValue(nextLiveKey, nextLiveValue);
        if (firstValue == null) {
            firstValue = nextLiveValue;
            firstValueKey = nextLiveKey;
        }
        lastValue = nextLiveValue;
        lastValueKey = nextLiveKey;
        inProgressSize++;
        if (inProgressSize >= batchFlushInterval) {
            flushLiveSegment();
        }
    }

    @Override
    public V getNextValue(final FDate date, final int shiftForwardUnits) {
        if (inProgress.isEmpty()) {
            //no live segment, go with historical
            return persistent.getNextValue(date, shiftForwardUnits);
        } else if (persistent.isEmpty() || inProgress.getFirstValueKey().isBefore(date)) {
            //live segment is after requested range, go with live
            final V nextValue = inProgress.getNextValue(date, shiftForwardUnits);
            return nextValue;
        } else {
            //use both segments
            V nextValue = null;
            try (ICloseableIterator<V> rangeValues = rangeValues(date, null).iterator()) {
                for (int i = 0; i < shiftForwardUnits; i++) {
                    nextValue = rangeValues.next();
                }
            } catch (final NoSuchElementException e) {
                //ignore
            }
            return nextValue;
        }
    }

    @Override
    public V getLatestValue(final FDate date) {
        if (inProgress.isEmpty()) {
            return persistent.getLatestValue(date);
        } else if (persistent.isEmpty()) {
            return inProgress.getLatestValue(date);
        }
        V latestValue = null;
        for (int i = 0; i < latestValueProviders.size(); i++) {
            final ILiveSegment<K, V> latestValueProvider = latestValueProviders.get(i);
            final V newValue = latestValueProvider.getLatestValue(date);
            final FDate newValueTime = historicalSegmentTable.extractTime(newValue);
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
        return firstValue == null;
    }

    @Override
    public void close() {
        firstValue = null;
        lastValue = null;
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
        persistent
                .putNextLiveValues(inProgress.rangeValues(inProgress.getFirstValueKey(), inProgress.getLastValueKey()));
        inProgressSize = 0;
        inProgress.close();
    }

}
