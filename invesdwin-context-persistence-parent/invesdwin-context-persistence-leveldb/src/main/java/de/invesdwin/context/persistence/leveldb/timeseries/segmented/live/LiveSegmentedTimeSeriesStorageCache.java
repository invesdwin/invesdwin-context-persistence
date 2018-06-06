package de.invesdwin.context.persistence.leveldb.timeseries.segmented.live;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.leveldb.timeseries.segmented.ASegmentedTimeSeriesStorageCache;
import de.invesdwin.context.persistence.leveldb.timeseries.segmented.SegmentedKey;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.FlatteningIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.time.TimeRange;
import de.invesdwin.util.time.fdate.FDate;

@ThreadSafe
public class LiveSegmentedTimeSeriesStorageCache<K, V> {

    private final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable;
    private final K key;
    private LiveSegment<K, V> liveSegment;
    private final Function<FDate, V> liveSegmentLatestValueProvider = new Function<FDate, V>() {
        @Override
        public V apply(final FDate t) {
            return liveSegment.getLatestValue(t);
        }
    };
    private final Function<FDate, V> historicalSegmentLatestValueProvider = new Function<FDate, V>() {
        @Override
        public V apply(final FDate t) {
            return historicalSegmentTable.getLatestValue(key, t);
        }
    };
    private final List<Function<FDate, V>> latestValueProviders = Arrays.asList(liveSegmentLatestValueProvider,
            historicalSegmentLatestValueProvider);

    public LiveSegmentedTimeSeriesStorageCache(
            final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable, final K key) {
        this.historicalSegmentTable = historicalSegmentTable;
        this.key = key;
    }

    public boolean isEmptyOrInconsistent() {
        if (liveSegment != null && liveSegment.isEmpty()) {
            return true;
        }
        return historicalSegmentTable.isEmptyOrInconsistent(key);
    }

    public void deleteAll() {
        historicalSegmentTable.deleteRange(key);
        liveSegment = null;
    }

    public V getFirstValue() {
        final V firstHistoricalValue = historicalSegmentTable.getLatestValue(key, FDate.MIN_DATE);
        if (firstHistoricalValue != null) {
            return firstHistoricalValue;
        } else if (liveSegment != null) {
            return liveSegment.getFirstValue();
        }
        return null;
    }

    public V getLastValue() {
        if (liveSegment != null) {
            final V lastLiveValue = liveSegment.getLastValue();
            if (lastLiveValue != null) {
                return lastLiveValue;
            }
        }
        return historicalSegmentTable.getLatestValue(key, FDate.MAX_DATE);
    }

    public ICloseableIterable<V> readRangeValues(final FDate from, final FDate to) {
        if (liveSegment == null) {
            //no live segment, go with historical
            return historicalSegmentTable.rangeValues(key, from, to);
        } else {
            final FDate liveSegmentFrom = liveSegment.getSegmentedKey().getSegment().getFrom();
            if (liveSegmentFrom.isAfter(to)) {
                //live segment is after requested range, go with historical
                return historicalSegmentTable.rangeValues(key, from, to);
            } else if (liveSegmentFrom.isBeforeOrEqualTo(from)) {
                //historical segment is before requested range, go with live
                return liveSegment.rangeValues(from, to);
            } else {
                //use both segments
                final ICloseableIterable<V> historicalRangeValues = historicalSegmentTable.rangeValues(key, from,
                        liveSegmentFrom.addMilliseconds(-1));
                final ICloseableIterable<V> liveRangeValues = liveSegment.rangeValues(liveSegmentFrom, to);
                return new FlatteningIterable<V>(historicalRangeValues, liveRangeValues);
            }
        }
    }

    public ICloseableIterable<V> readRangeValuesReverse(final FDate from, final FDate to) {
        if (liveSegment == null) {
            //no live segment, go with historical
            return historicalSegmentTable.rangeReverseValues(key, from, to);
        } else {
            final FDate liveSegmentFrom = liveSegment.getSegmentedKey().getSegment().getFrom();
            if (liveSegmentFrom.isAfter(from)) {
                //live segment is after requested range, go with historical
                return historicalSegmentTable.rangeReverseValues(key, from, to);
            } else if (liveSegmentFrom.isBeforeOrEqualTo(to)) {
                //historical segment is before requested range, go with live
                return liveSegment.rangeReverseValues(from, to);
            } else {
                //use both segments
                final ICloseableIterable<V> liveRangeValues = liveSegment.rangeReverseValues(from, liveSegmentFrom);
                final ICloseableIterable<V> historicalRangeValues = historicalSegmentTable.rangeReverseValues(key,
                        liveSegmentFrom.addMilliseconds(-1), to);
                return new FlatteningIterable<V>(liveRangeValues, historicalRangeValues);
            }
        }
    }

    public V getLatestValue(final FDate date) {
        if (liveSegment == null) {
            return historicalSegmentLatestValueProvider.apply(date);
        }
        V latestValue = null;
        for (int i = 0; i < latestValueProviders.size(); i++) {
            final Function<FDate, V> latestValueProvider = latestValueProviders.get(i);
            final V newValue = latestValueProvider.apply(date);
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

    public V getPreviousValue(final FDate date, final int shiftBackUnits) {
        if (liveSegment == null) {
            //no live segment, go with historical
            return historicalSegmentTable.getPreviousValue(key, date, shiftBackUnits);
        } else if (liveSegment.getSegmentedKey().getSegment().getFrom().isAfter(date)) {
            //live segment is after requested range, go with historical
            return historicalSegmentTable.getPreviousValue(key, date, shiftBackUnits);
        } else {
            //use both segments
            V previousValue = null;
            try (ICloseableIterator<V> rangeValuesReverse = readRangeValuesReverse(date, null).iterator()) {
                for (int i = 0; i < shiftBackUnits; i++) {
                    previousValue = rangeValuesReverse.next();
                }
            } catch (final NoSuchElementException e) {
                //ignore
            }
            return previousValue;
        }
    }

    public V getNextValue(final FDate date, final int shiftForwardUnits) {
        if (liveSegment == null) {
            //no live segment, go with historical
            return historicalSegmentTable.getNextValue(key, date, shiftForwardUnits);
        } else if (liveSegment.getSegmentedKey().getSegment().getFrom().isBefore(date)) {
            //live segment is after requested range, go with live
            final V nextValue = liveSegment.getNextValue(date, shiftForwardUnits);
            return nextValue;
        } else {
            //use both segments
            V nextValue = null;
            try (ICloseableIterator<V> rangeValues = readRangeValues(date, null).iterator()) {
                for (int i = 0; i < shiftForwardUnits; i++) {
                    nextValue = rangeValues.next();
                }
            } catch (final NoSuchElementException e) {
                //ignore
            }
            return nextValue;
        }
    }

    public void putNextLiveValue(final V nextLiveValue) {
        final FDate nextLiveKey = historicalSegmentTable.extractTime(nextLiveValue);
        final FDate lastAvailableHistoricalSegmentTo = historicalSegmentTable.getLastAvailableHistoricalSegmentTo(key);
        final TimeRange segment = historicalSegmentTable.getSegmentFinder(key).query().getValue(nextLiveKey);
        if (lastAvailableHistoricalSegmentTo.isAfterOrEqualTo(segment.getFrom())) {
            throw new IllegalStateException("lastAvailableHistoricalSegmentTo [" + lastAvailableHistoricalSegmentTo
                    + "] should be before liveSegmentFrom [" + segment.getFrom() + "]");
        }
        if (liveSegment != null && nextLiveKey.isAfter(liveSegment.getSegmentedKey().getSegment().getTo())) {
            if (!lastAvailableHistoricalSegmentTo.equals(liveSegment.getSegmentedKey().getSegment().getTo())) {
                throw new IllegalStateException("lastAvailableHistoricalSegmentTo [" + lastAvailableHistoricalSegmentTo
                        + "] should be equal to liveSegmentTo [" + segment.getFrom() + "]");
            }
            convertLiveSegmentToHistorical();
            liveSegment = null;
        }
        if (liveSegment == null) {
            final SegmentedKey<K> segmentedKey = new SegmentedKey<K>(key, segment);
            liveSegment = new LiveSegment<>(segmentedKey);
        }
        liveSegment.putNextLiveValue(nextLiveKey, nextLiveValue);
    }

    private void convertLiveSegmentToHistorical() {
        final ASegmentedTimeSeriesStorageCache<K, V> lookupTableCache = historicalSegmentTable.getLookupTableCache(key);
        final boolean initialized = lookupTableCache.maybeInitSegment(liveSegment.getSegmentedKey(),
                new Function<SegmentedKey<K>, ICloseableIterable<? extends V>>() {
                    @Override
                    public ICloseableIterable<? extends V> apply(final SegmentedKey<K> t) {
                        return liveSegment.rangeValues(t.getSegment().getFrom(), t.getSegment().getTo());
                    }
                });
        Assertions.checkTrue(initialized);
    }

}
