package de.invesdwin.context.persistence.leveldb.timeseries.segmented;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.persistence.leveldb.ezdb.ADelegateRangeTable;
import de.invesdwin.context.persistence.leveldb.ezdb.ADelegateRangeTable.DelegateTableIterator;
import de.invesdwin.context.persistence.leveldb.timeseries.ATimeSeriesDB;
import de.invesdwin.context.persistence.leveldb.timeseries.ITimeSeriesDB;
import de.invesdwin.context.persistence.leveldb.timeseries.storage.TimeSeriesStorage;
import de.invesdwin.util.collections.iterable.ACloseableIterator;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import de.invesdwin.util.collections.loadingcache.historical.AHistoricalCache;
import de.invesdwin.util.time.TimeRange;
import de.invesdwin.util.time.fdate.FDate;
import ezdb.TableRow;
import ezdb.serde.Serde;

@ThreadSafe
public abstract class ASegmentedTimeSeriesDB<K, V> implements ITimeSeriesDB<K, V> {

    private final SegmentedTable segmentedTable;
    private final ALoadingCache<K, ReadWriteLock> key_tableLock = new ALoadingCache<K, ReadWriteLock>() {
        @Override
        protected ReadWriteLock loadValue(final K key) {
            return new ReentrantReadWriteLock();
        }
    };
    private final AHistoricalCache<TimeRange> segmentFinder;
    private final ALoadingCache<K, ASegmentedTimeSeriesStorageCache<K, V>> key_lookupTableCache;

    public ASegmentedTimeSeriesDB(final String name, final AHistoricalCache<TimeRange> segmentFinder) {
        this.segmentedTable = new SegmentedTable(name);
        this.segmentFinder = segmentFinder;
        this.key_lookupTableCache = new ALoadingCache<K, ASegmentedTimeSeriesStorageCache<K, V>>() {
            @Override
            protected ASegmentedTimeSeriesStorageCache<K, V> loadValue(final K key) {
                final String hashKey = hashKeyToString(key);
                return new ASegmentedTimeSeriesStorageCache<K, V>(segmentedTable, getStorage(), key, hashKey,
                        segmentFinder) {

                    @Override
                    protected FDate getLastAvailableSegmentTo(final K key) {
                        return ASegmentedTimeSeriesDB.this.getLastAvailableSegmentTo(key);
                    }

                    @Override
                    protected FDate getFirstAvailableSegmentFrom(final K key) {
                        return ASegmentedTimeSeriesDB.this.getFirstAvailableSegmentFrom(key);
                    }

                    @Override
                    protected void maybeInitSegment(final SegmentedKey<K> segmentedKey) {
                        //1. check segment status in series storage
                        //2. if not existing or false, set status to false -> start segment update -> after update set status to true
                        //      throw error if a segment is being updated that is beyond the lastAvailableSegmentTo
                        //3. if true do nothing
                        System.out.println("TODO");
                    }

                };
            }

        };
    }

    protected SegmentedTimeSeriesStorage newStorage(final File directory) {
        return new SegmentedTimeSeriesStorage(directory);
    }

    protected void deleteCorruptedStorage(final File directory) {
        directory.delete();
    }

    public AHistoricalCache<TimeRange> getTimeRangeFinder() {
        return segmentFinder;
    }

    protected SegmentedTimeSeriesStorage getStorage() {
        return (SegmentedTimeSeriesStorage) segmentedTable.getStorage();
    }

    protected abstract Integer newFixedLength();

    protected abstract Serde<V> newValueSerde();

    protected abstract FDate extractTime(V value);

    protected abstract String hashKeyToString(K key);

    protected abstract FDate getFirstAvailableSegmentFrom(K key);

    protected abstract FDate getLastAvailableSegmentTo(K key);

    protected final class SegmentedTable extends ATimeSeriesDB<SegmentedKey<K>, V> {
        private SegmentedTable(final String name) {
            super(name);
        }

        @Override
        public Serde<V> getValueSerde() {
            return super.getValueSerde();
        }

        @Override
        protected Integer newFixedLength() {
            return ASegmentedTimeSeriesDB.this.newFixedLength();
        }

        @Override
        public Integer getFixedLength() {
            return super.getFixedLength();
        }

        @Override
        protected Serde<V> newValueSerde() {
            return ASegmentedTimeSeriesDB.this.newValueSerde();
        }

        @Override
        public FDate extractTime(final V value) {
            return ASegmentedTimeSeriesDB.this.extractTime(value);
        }

        @Override
        public String hashKeyToString(final SegmentedKey<K> key) {
            return ASegmentedTimeSeriesDB.this.hashKeyToString(key.getKey()) + "/"
                    + key.getSegment().getFrom().toString(FDate.FORMAT_TIMESTAMP_UNDERSCORE) + "-"
                    + key.getSegment().getTo().toString(FDate.FORMAT_TIMESTAMP_UNDERSCORE);
        }

        @Override
        protected TimeSeriesStorage newStorage(final File directory) {
            return ASegmentedTimeSeriesDB.this.newStorage(directory);
        }

        @Override
        protected void deleteCorruptedStorage(final File directory) {
            ASegmentedTimeSeriesDB.this.deleteCorruptedStorage(directory);
        }

        @Override
        public synchronized TimeSeriesStorage getStorage() {
            return super.getStorage();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        segmentedTable.close();
        key_lookupTableCache.clear();
        key_tableLock.clear();
    }

    @Override
    public File getDirectory() {
        return segmentedTable.getDirectory();
    }

    @Override
    public ReadWriteLock getTableLock(final K key) {
        return key_tableLock.get(key);
    }

    @Override
    public boolean isEmptyOrInconsistent(final K key) {
        final Lock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            return getLookupTableCache(key).isEmptyOrInconsistent();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void deleteRange(final K key) {
        final Lock writeLock = getTableLock(key).writeLock();
        try {
            if (!writeLock.tryLock(1, TimeUnit.MINUTES)) {
                throw new RetryLaterRuntimeException("Write lock could not be acquired for table [" + getName()
                        + "] and key [" + key + "]. Please ensure all iterators are closed!");
            }
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            final ADelegateRangeTable<String, TimeRange, Boolean> segmentsTable = getStorage().getSegmentsTable();
            final String hashKey = hashKeyToString(key);
            try (DelegateTableIterator<String, TimeRange, Boolean> range = segmentsTable.range(hashKey)) {
                while (true) {
                    final TableRow<String, TimeRange, Boolean> row = range.next();
                    segmentedTable.deleteRange(new SegmentedKey<K>(key, row.getRangeKey()));
                }
            } catch (final NoSuchElementException e) {
                //end reached
            }
            segmentsTable.deleteRange(hashKey);
            getLookupTableCache(key).deleteAll();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public String getName() {
        return segmentedTable.getName();
    }

    private ASegmentedTimeSeriesStorageCache<K, V> getLookupTableCache(final K key) {
        return key_lookupTableCache.get(key);
    }

    @Override
    public ICloseableIterator<V> rangeValues(final K key, final FDate from, final FDate to) {
        return new ACloseableIterator<V>() {

            private final Lock readLock = getTableLock(key).readLock();
            private ICloseableIterator<V> readRangeValues;

            private ICloseableIterator<V> getReadRangeValues() {
                if (readRangeValues == null) {
                    readLock.lock();
                    readRangeValues = getLookupTableCache(key).readRangeValues(from, to).iterator();
                }
                return readRangeValues;
            }

            @Override
            public boolean innerHasNext() {
                return getReadRangeValues().hasNext();
            }

            @Override
            public V innerNext() {
                return getReadRangeValues().next();
            }

            @Override
            public void innerClose() {
                if (readRangeValues instanceof EmptyCloseableIterator) {
                    //already closed
                    return;
                }
                if (readRangeValues != null) {
                    getReadRangeValues().close();
                    readLock.unlock();
                }
                readRangeValues = EmptyCloseableIterator.getInstance();
            }
        };
    }

    @Override
    public ICloseableIterator<V> rangeReverseValues(final K key, final FDate from, final FDate to) {
        return new ACloseableIterator<V>() {

            private final Lock readLock = getTableLock(key).readLock();
            private ICloseableIterator<V> readRangeValues;

            private ICloseableIterator<V> getReadRangeValues() {
                if (readRangeValues == null) {
                    readLock.lock();
                    readRangeValues = getLookupTableCache(key).readRangeValuesReverse(from, to).iterator();
                }
                return readRangeValues;
            }

            @Override
            public boolean innerHasNext() {
                return getReadRangeValues().hasNext();
            }

            @Override
            public V innerNext() {
                return getReadRangeValues().next();
            }

            @Override
            public void innerClose() {
                if (readRangeValues instanceof EmptyCloseableIterator) {
                    //already closed
                    return;
                }
                if (readRangeValues != null) {
                    getReadRangeValues().close();
                    readLock.unlock();
                }
                readRangeValues = EmptyCloseableIterator.getInstance();
            }
        };
    }

    @Override
    public FDate getLatestValueKey(final K key, final FDate date) {
        final V value = getLatestValue(key, date);
        if (value == null) {
            return null;
        } else {
            return extractTime(value);
        }
    }

    @Override
    public V getLatestValue(final K key, final FDate date) {
        final Lock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            if (date.isBeforeOrEqualTo(FDate.MIN_DATE)) {
                return getLookupTableCache(key).getFirstValue();
            } else if (date.isAfterOrEqualTo(FDate.MAX_DATE)) {
                return getLookupTableCache(key).getLastValue();
            } else {
                return getLookupTableCache(key).getLatestValue(date);
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public FDate getPreviousValueKey(final K key, final FDate date, final int shiftBackUnits) {
        final V value = getPreviousValue(key, date, shiftBackUnits);
        if (value == null) {
            return null;
        } else {
            return extractTime(value);
        }
    }

    @Override
    public V getPreviousValue(final K key, final FDate date, final int shiftBackUnits) {
        final Lock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            if (date.isBeforeOrEqualTo(FDate.MIN_DATE)) {
                return getLookupTableCache(key).getFirstValue();
            } else {
                return getLookupTableCache(key).getPreviousValue(date, shiftBackUnits);
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public FDate getNextValueKey(final K key, final FDate date, final int shiftForwardUnits) {
        final V value = getNextValue(key, date, shiftForwardUnits);
        if (value == null) {
            return null;
        } else {
            return extractTime(value);
        }
    }

    @Override
    public V getNextValue(final K key, final FDate date, final int shiftForwardUnits) {
        final Lock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            if (date.isAfterOrEqualTo(FDate.MAX_DATE)) {
                return getLookupTableCache(key).getLastValue();
            } else {
                return getLookupTableCache(key).getNextValue(date, shiftForwardUnits);
            }
        } finally {
            readLock.unlock();
        }
    }

}
