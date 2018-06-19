package de.invesdwin.context.persistence.leveldb.timeseries.segmented.live.internal;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.leveldb.timeseries.ATimeSeriesUpdater;
import de.invesdwin.context.persistence.leveldb.timeseries.segmented.ASegmentedTimeSeriesStorageCache;
import de.invesdwin.context.persistence.leveldb.timeseries.segmented.SegmentedKey;
import de.invesdwin.context.persistence.leveldb.timeseries.segmented.live.ALiveSegmentedTimeSeriesDB;
import de.invesdwin.util.collections.iterable.FlatteningIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.time.fdate.FDate;

@NotThreadSafe
public class SwitchingLiveSegment<K, V> implements ILiveSegment<K, V> {

    private final SegmentedKey<K> segmentedKey;
    private final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable;
    private final MemoryLiveSegment<K, V> memory;
    private final PersistentLiveSegment<K, V> persistent;
    private final List<ILiveSegment<K, V>> latestValueProviders;

    private FDate firstValueKey;
    private V firstValue;
    private FDate lastValueKey;
    private V lastValue;
    private int memorySize = 0;

    public SwitchingLiveSegment(final SegmentedKey<K> segmentedKey,
            final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable) {
        this.segmentedKey = segmentedKey;
        this.historicalSegmentTable = historicalSegmentTable;
        this.memory = new MemoryLiveSegment<>(segmentedKey, historicalSegmentTable);
        this.persistent = new PersistentLiveSegment<>(segmentedKey, historicalSegmentTable);
        this.latestValueProviders = Arrays.asList(memory, persistent);
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
        if (memory.isEmpty()) {
            //no live segment, go with historical
            return persistent.rangeValues(from, to);
        } else {
            final FDate memoryFrom = memory.getFirstValueKey();
            if (memoryFrom.isAfter(to)) {
                //live segment is after requested range, go with historical
                return persistent.rangeValues(from, to);
            } else if (memoryFrom.isBeforeOrEqualTo(from)) {
                //historical segment is before requested range, go with live
                return memory.rangeValues(from, to);
            } else {
                //use both segments
                final ICloseableIterable<V> historicalRangeValues = persistent.rangeValues(from,
                        memoryFrom.addMilliseconds(-1));
                final ICloseableIterable<V> liveRangeValues = memory.rangeValues(memoryFrom, to);
                return new FlatteningIterable<V>(historicalRangeValues, liveRangeValues);
            }
        }
    }

    @Override
    public ICloseableIterable<V> rangeReverseValues(final FDate from, final FDate to) {
        if (memory.isEmpty()) {
            //no live segment, go with historical
            return persistent.rangeReverseValues(from, to);
        } else {
            final FDate memoryFrom = memory.getFirstValueKey();
            if (memoryFrom.isAfter(from)) {
                //live segment is after requested range, go with historical
                return persistent.rangeReverseValues(from, to);
            } else if (memoryFrom.isBeforeOrEqualTo(to)) {
                //historical segment is before requested range, go with live
                return memory.rangeReverseValues(from, to);
            } else {
                //use both segments
                final ICloseableIterable<V> liveRangeValues = memory.rangeReverseValues(from, memoryFrom);
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
        memory.putNextLiveValue(nextLiveKey, nextLiveValue);
        if (firstValue == null) {
            firstValue = nextLiveValue;
            firstValueKey = nextLiveKey;
        }
        lastValue = nextLiveValue;
        lastValueKey = nextLiveKey;
        memorySize++;
        if (memorySize >= ATimeSeriesUpdater.BATCH_FLUSH_INTERVAL) {
            persistent.putNextLiveValues(memory.rangeValues(memory.getFirstValueKey(), memory.getLastValueKey()));
            memorySize = 0;
            memory.close();
        }
    }

    @Override
    public V getNextValue(final FDate date, final int shiftForwardUnits) {
        if (memory.isEmpty()) {
            //no live segment, go with historical
            return persistent.getNextValue(date, shiftForwardUnits);
        } else if (memory.getFirstValueKey().isBefore(date)) {
            //live segment is after requested range, go with live
            final V nextValue = memory.getNextValue(date, shiftForwardUnits);
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
        if (memory.isEmpty()) {
            return persistent.getLatestValue(date);
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
        memory.close();
        persistent.close();
    }

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
