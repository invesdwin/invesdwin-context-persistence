package de.invesdwin.context.persistence.leveldb.timeseries.segmented;

import java.util.NoSuchElementException;
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang3.SerializationException;

import com.google.common.base.Function;

import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.leveldb.ezdb.ADelegateRangeTable;
import de.invesdwin.context.persistence.leveldb.ezdb.ADelegateRangeTable.DelegateTableIterator;
import de.invesdwin.context.persistence.leveldb.timeseries.TimeSeriesStorageCache;
import de.invesdwin.context.persistence.leveldb.timeseries.storage.ChunkValue;
import de.invesdwin.context.persistence.leveldb.timeseries.storage.ShiftUnitsRangeKey;
import de.invesdwin.context.persistence.leveldb.timeseries.storage.SingleValue;
import de.invesdwin.util.bean.tuple.Pair;
import de.invesdwin.util.collections.eviction.EvictionMode;
import de.invesdwin.util.collections.iterable.ASkippingIterable;
import de.invesdwin.util.collections.iterable.ATransformingCloseableIterable;
import de.invesdwin.util.collections.iterable.FlatteningIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import de.invesdwin.util.collections.loadingcache.historical.AHistoricalCache;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.time.TimeRange;
import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FDates;
import ezdb.TableRow;
import ezdb.serde.Serde;

@ThreadSafe
public abstract class ASegmentedTimeSeriesStorageCache<K, V> {
    public static final Integer MAXIMUM_SIZE = TimeSeriesStorageCache.MAXIMUM_SIZE;
    public static final EvictionMode EVICTION_MODE = TimeSeriesStorageCache.EVICTION_MODE;

    private final ALoadingCache<FDate, V> latestValueLookupCache = new ALoadingCache<FDate, V>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return MAXIMUM_SIZE;
        }

        @Override
        protected EvictionMode getEvictionMode() {
            return EVICTION_MODE;
        }

        @Override
        protected V loadValue(final FDate date) {
            final SingleValue value = storage.getLatestValueLookupTable().getOrLoad(hashKey, date,
                    new Function<Pair<String, FDate>, SingleValue>() {

                        @Override
                        public SingleValue apply(final Pair<String, FDate> input) {
                            final FDate firstAvailableSegmentFrom = getFirstAvailableSegmentFrom(key);
                            final FDate lastAvailableSegmentTo = getLastAvailableSegmentTo(key);
                            final FDate adjFrom = firstAvailableSegmentFrom;
                            final FDate adjTo = FDates.min(input.getSecond(), lastAvailableSegmentTo);
                            final ICloseableIterable<TimeRange> segmentsReverse = getSegmentsReverse(adjFrom, adjTo);
                            try (ICloseableIterator<TimeRange> it = segmentsReverse.iterator()) {
                                V latestValue = null;
                                while (it.hasNext()) {
                                    final TimeRange segment = it.next();
                                    final SegmentedKey<K> segmentedKey = new SegmentedKey<K>(key, segment);
                                    maybeInitSegment(segmentedKey);
                                    final V newValue = segmentedTable.getLatestValue(segmentedKey, date);
                                    final FDate newValueTime = segmentedTable.extractTime(newValue);
                                    if (newValueTime.isAfter(date)) {
                                        /*
                                         * even if we got the first value in this segment and it is after the desired
                                         * key we just continue to the beginning to search for an earlier value until we
                                         * reach the overall firstValue
                                         */
                                        break;
                                    } else {
                                        latestValue = newValue;
                                    }
                                }
                                if (latestValue == null) {
                                    latestValue = getFirstValue();
                                }
                                return new SingleValue(valueSerde, latestValue);
                            }
                        }
                    });
            if (value == null) {
                return null;
            }
            return value.getValue(valueSerde);
        }
    };
    private final ALoadingCache<Pair<FDate, Integer>, V> previousValueLookupCache = new ALoadingCache<Pair<FDate, Integer>, V>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return MAXIMUM_SIZE;
        }

        @Override
        protected EvictionMode getEvictionMode() {
            return EVICTION_MODE;
        }

        @Override
        protected V loadValue(final Pair<FDate, Integer> loadKey) {
            final FDate date = loadKey.getFirst();
            final int shiftBackUnits = loadKey.getSecond();
            final SingleValue value = storage.getPreviousValueLookupTable().getOrLoad(hashKey,
                    new ShiftUnitsRangeKey(date, shiftBackUnits),
                    new Function<Pair<String, ShiftUnitsRangeKey>, SingleValue>() {

                        @Override
                        public SingleValue apply(final Pair<String, ShiftUnitsRangeKey> input) {
                            final FDate date = loadKey.getFirst();
                            final int shiftBackUnits = loadKey.getSecond();
                            V previousValue = null;
                            try (ICloseableIterator<V> rangeValuesReverse = readRangeValuesReverse(date, null)
                                    .iterator()) {
                                for (int i = 0; i < shiftBackUnits; i++) {
                                    previousValue = rangeValuesReverse.next();
                                }
                            } catch (final NoSuchElementException e) {
                                //ignore
                            }
                            return new SingleValue(valueSerde, previousValue);
                        }
                    });
            return value.getValue(valueSerde);
        }
    };
    private final ALoadingCache<Pair<FDate, Integer>, V> nextValueLookupCache = new ALoadingCache<Pair<FDate, Integer>, V>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return MAXIMUM_SIZE;
        }

        @Override
        protected EvictionMode getEvictionMode() {
            return EVICTION_MODE;
        }

        @Override
        protected V loadValue(final Pair<FDate, Integer> loadKey) {
            final FDate date = loadKey.getFirst();
            final int shiftForwardUnits = loadKey.getSecond();
            final SingleValue value = storage.getNextValueLookupTable().getOrLoad(hashKey,
                    new ShiftUnitsRangeKey(date, shiftForwardUnits),
                    new Function<Pair<String, ShiftUnitsRangeKey>, SingleValue>() {

                        @Override
                        public SingleValue apply(final Pair<String, ShiftUnitsRangeKey> input) {
                            final FDate date = loadKey.getFirst();
                            final int shiftForwardUnits = loadKey.getSecond();
                            V nextValue = null;
                            try (ICloseableIterator<V> rangeValues = readRangeValues(date, null).iterator()) {
                                for (int i = 0; i < shiftForwardUnits; i++) {
                                    nextValue = rangeValues.next();
                                }
                            } catch (final NoSuchElementException e) {
                                //ignore
                            }
                            return new SingleValue(valueSerde, nextValue);
                        }
                    });
            return value.getValue(valueSerde);
        }
    };

    private volatile Optional<V> cachedFirstValue;
    private volatile Optional<V> cachedLastValue;
    private final Log log = new Log(this);

    private final AHistoricalCache<TimeRange> segmentFinder;
    private final ASegmentedTimeSeriesDB<K, V>.SegmentedTable segmentedTable;
    private final SegmentedTimeSeriesStorage storage;
    private final K key;
    private final String hashKey;
    private final Serde<V> valueSerde;

    public ASegmentedTimeSeriesStorageCache(final ASegmentedTimeSeriesDB<K, V>.SegmentedTable segmentedTable,
            final SegmentedTimeSeriesStorage storage, final K key, final String hashKey,
            final AHistoricalCache<TimeRange> segmentFinder) {
        this.storage = storage;
        this.segmentedTable = segmentedTable;
        this.key = key;
        this.hashKey = hashKey;
        this.segmentFinder = segmentFinder;
        this.valueSerde = segmentedTable.getValueSerde();
    }

    public ICloseableIterable<V> readRangeValues(final FDate from, final FDate to) {
        final FDate firstAvailableSegmentFrom = getFirstAvailableSegmentFrom(key);
        final FDate lastAvailableSegmentTo = getLastAvailableSegmentTo(key);
        //adjust dates directly to prevent unnecessary segment calculations
        final FDate adjFrom = FDates.max(from, firstAvailableSegmentFrom);
        final FDate adjTo = FDates.min(to, lastAvailableSegmentTo);
        final ICloseableIterable<TimeRange> segments = getSegments(adjFrom, adjTo);
        final ATransformingCloseableIterable<TimeRange, ICloseableIterable<V>> segmentQueries = new ATransformingCloseableIterable<TimeRange, ICloseableIterable<V>>(
                segments) {
            @Override
            protected ICloseableIterable<V> transform(final TimeRange value) {
                return new ICloseableIterable<V>() {
                    @Override
                    public ICloseableIterator<V> iterator() {
                        final SegmentedKey<K> segmentedKey = new SegmentedKey<K>(key, value);
                        maybeInitSegment(segmentedKey);
                        final FDate segmentAdjFrom = FDates.max(adjFrom, value.getFrom());
                        final FDate segmentAdjTo = FDates.min(adjTo, value.getTo());
                        return segmentedTable.rangeValues(segmentedKey, segmentAdjFrom, segmentAdjTo);
                    }

                };
            }
        };
        final ICloseableIterable<V> rangeValues = new FlatteningIterable<V>(segmentQueries);
        return rangeValues;
    }

    private ICloseableIterable<TimeRange> getSegments(final FDate adjFrom, final FDate adjTo) {
        final ICloseableIterable<TimeRange> segments = segmentFinder.query().getValues(adjFrom, adjTo);
        final ASkippingIterable<TimeRange> filteredSegments = new ASkippingIterable<TimeRange>(segments) {
            @Override
            protected boolean skip(final TimeRange element) {
                //though additionally skip ranges that exceed the available dates
                final FDate segmentTo = element.getTo();
                if (segmentTo.isBefore(adjFrom)) {
                    throw new IllegalStateException(
                            "segmentTo [" + segmentTo + "] should not be before adjFrom [" + adjFrom + "]");
                }
                if (segmentTo.isAfter(adjTo)) {
                    //no need to continue going higher
                    throw new FastNoSuchElementException("ASegmentedTimeSeriesStorageCache getSegments end reached");
                }
                return false;
            }
        };
        return filteredSegments;
    }

    protected abstract void maybeInitSegment(SegmentedKey<K> segmentedKey);

    protected abstract FDate getLastAvailableSegmentTo(K key);

    protected abstract FDate getFirstAvailableSegmentFrom(K key);

    protected ICloseableIterable<V> readRangeValuesReverse(final FDate from, final FDate to) {
        final FDate firstAvailableSegmentFrom = getFirstAvailableSegmentFrom(key);
        final FDate lastAvailableSegmentTo = getLastAvailableSegmentTo(key);
        //adjust dates directly to prevent unnecessary segment calculations
        final FDate adjFrom = FDates.max(from, firstAvailableSegmentFrom);
        final FDate adjTo = FDates.min(to, lastAvailableSegmentTo);
        final ICloseableIterable<TimeRange> filteredSegments = getSegmentsReverse(adjFrom, adjTo);
        final ATransformingCloseableIterable<TimeRange, ICloseableIterable<V>> segmentQueries = new ATransformingCloseableIterable<TimeRange, ICloseableIterable<V>>(
                filteredSegments) {
            @Override
            protected ICloseableIterable<V> transform(final TimeRange value) {
                return new ICloseableIterable<V>() {
                    @Override
                    public ICloseableIterator<V> iterator() {
                        final SegmentedKey<K> segmentedKey = new SegmentedKey<K>(key, value);
                        maybeInitSegment(segmentedKey);
                        final FDate segmentAdjFrom = FDates.max(adjFrom, value.getFrom());
                        final FDate segmentAdjTo = FDates.min(adjTo, value.getTo());
                        return segmentedTable.rangeReverseValues(segmentedKey, segmentAdjFrom, segmentAdjTo);
                    }

                };
            }
        };
        final ICloseableIterable<V> rangeValues = new FlatteningIterable<V>(segmentQueries);
        return rangeValues;
    }

    private ICloseableIterable<TimeRange> getSegmentsReverse(final FDate adjFrom, final FDate adjTo) {
        final ICloseableIterable<TimeRange> segments = new ICloseableIterable<TimeRange>() {
            @Override
            public ICloseableIterator<TimeRange> iterator() {
                return new ICloseableIterator<TimeRange>() {

                    private TimeRange curSegment = segmentFinder.query().getValue(adjTo);

                    @Override
                    public boolean hasNext() {
                        return curSegment.getTo().isAfter(adjFrom);
                    }

                    @Override
                    public TimeRange next() {
                        final TimeRange next = curSegment;
                        //get one segment earlier
                        curSegment = segmentFinder.query().getValue(curSegment.getFrom().addMilliseconds(-1));
                        return next;
                    }

                    @Override
                    public void close() {
                        curSegment = new TimeRange(FDate.MIN_DATE, FDate.MIN_DATE);
                    }
                };
            }
        };
        final ASkippingIterable<TimeRange> filteredSegments = new ASkippingIterable<TimeRange>(segments) {
            @Override
            protected boolean skip(final TimeRange element) {
                //though additionally skip ranges that exceed the available dates
                final FDate segmentTo = element.getTo();
                if (segmentTo.isBefore(adjFrom)) {
                    //no need to continue going lower
                    throw new FastNoSuchElementException("ASegmentedTimeSeriesStorageCache getSegments end reached");
                }
                //skip last value and continue with earlier ones
                return segmentTo.isAfter(adjTo);
            }
        };
        return filteredSegments;
    }

    public synchronized void deleteAll() {
        storage.getLatestValueLookupTable().deleteRange(hashKey);
        storage.getNextValueLookupTable().deleteRange(hashKey);
        storage.getPreviousValueLookupTable().deleteRange(hashKey);
        clearCaches();
    }

    private void clearCaches() {
        latestValueLookupCache.clear();
        nextValueLookupCache.clear();
        previousValueLookupCache.clear();
        cachedFirstValue = null;
        cachedLastValue = null;
    }

    public V getLatestValue(final FDate date) {
        return latestValueLookupCache.get(date);
    }

    public V getPreviousValue(final FDate date, final int shiftBackUnits) {
        assertShiftUnitsPositiveNonZero(shiftBackUnits);
        return previousValueLookupCache.get(Pair.of(date, shiftBackUnits));
    }

    public V getNextValue(final FDate date, final int shiftForwardUnits) {
        assertShiftUnitsPositiveNonZero(shiftForwardUnits);
        return nextValueLookupCache.get(Pair.of(date, shiftForwardUnits));
    }

    public synchronized void prepareForUpdate() {
        final FDate lastTime = segmentedTable.extractTime(getLastValue());
        if (lastTime != null) {
            storage.getLatestValueLookupTable().deleteRange(hashKey, lastTime);
            storage.getNextValueLookupTable().deleteRange(hashKey); //we cannot be sure here about the date since shift keys can be arbitrarily large
            storage.getPreviousValueLookupTable().deleteRange(hashKey, new ShiftUnitsRangeKey(lastTime, 0));
        }
        clearCaches();
    }

    private void assertShiftUnitsPositiveNonZero(final int shiftUnits) {
        if (shiftUnits <= 0) {
            throw new IllegalArgumentException("shiftUnits needs to be a positive non zero value: " + shiftUnits);
        }
    }

    public V getFirstValue() {
        if (cachedFirstValue == null) {
            final FDate firstAvailableSegmentFrom = getFirstAvailableSegmentFrom(key);
            final TimeRange segment = segmentFinder.query().getValue(firstAvailableSegmentFrom);
            final SegmentedKey<K> segmentedKey = new SegmentedKey<K>(key, segment);
            maybeInitSegment(segmentedKey);
            final String segmentedHashKey = segmentedTable.hashKeyToString(segmentedKey);
            final ChunkValue latestValue = storage.getFileLookupTable().getLatestValue(segmentedHashKey,
                    FDate.MIN_DATE);
            final V firstValue;
            if (latestValue == null) {
                firstValue = null;
            } else {
                firstValue = latestValue.getFirstValue(valueSerde);
            }
            cachedFirstValue = Optional.ofNullable(firstValue);
        }
        return cachedFirstValue.orElse(null);
    }

    public V getLastValue() {
        if (cachedLastValue == null) {
            final FDate lastAvailableSegmentTo = getLastAvailableSegmentTo(key);
            final TimeRange segment = segmentFinder.query().getValue(lastAvailableSegmentTo);
            final SegmentedKey<K> segmentedKey = new SegmentedKey<K>(key, segment);
            maybeInitSegment(segmentedKey);
            final String segmentedHashKey = segmentedTable.hashKeyToString(segmentedKey);
            final ChunkValue latestValue = storage.getFileLookupTable().getLatestValue(segmentedHashKey,
                    FDate.MAX_DATE);
            final V lastValue;
            if (latestValue == null) {
                lastValue = null;
            } else {
                lastValue = latestValue.getLastValue(valueSerde);
            }
            cachedLastValue = Optional.ofNullable(lastValue);
        }
        return cachedLastValue.orElse(null);
    }

    public boolean isEmptyOrInconsistent() {
        try {
            getFirstValue();
            getLastValue();
        } catch (final Throwable t) {
            if (Throwables.isCausedByType(t, SerializationException.class)) {
                //e.g. fst: unable to find class for code 88 after version upgrade
                log.warn("Table data for [%s] is inconsistent and needs to be reset. Exception during getLastValue: %s",
                        hashKey, t.toString());
                return true;
            } else {
                //unexpected exception, since RemoteFastSerializingSerde only throws SerializingException
                throw Throwables.propagate(t);
            }
        }
        boolean empty = true;
        final ADelegateRangeTable<String, TimeRange, Boolean> segmentsTable = storage.getSegmentsTable();
        try (DelegateTableIterator<String, TimeRange, Boolean> range = segmentsTable.range(hashKey)) {
            while (true) {
                final TableRow<String, TimeRange, Boolean> row = range.next();
                if (segmentedTable.isEmptyOrInconsistent(new SegmentedKey<K>(key, row.getRangeKey()))) {
                    return true;
                }
                empty = false;
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
        return empty;
    }

}
