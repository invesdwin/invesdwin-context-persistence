package de.invesdwin.context.persistence.leveldb.timeseries.segmented.live;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.persistence.leveldb.timeseries.ATimeSeriesDB;
import de.invesdwin.context.persistence.leveldb.timeseries.ITimeSeriesDB;
import de.invesdwin.context.persistence.leveldb.timeseries.segmented.ASegmentedTimeSeriesDB;
import de.invesdwin.context.persistence.leveldb.timeseries.segmented.ASegmentedTimeSeriesStorageCache;
import de.invesdwin.context.persistence.leveldb.timeseries.segmented.SegmentedTimeSeriesStorage;
import de.invesdwin.util.collections.iterable.ACloseableIterator;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import de.invesdwin.util.collections.loadingcache.historical.AHistoricalCache;
import de.invesdwin.util.time.TimeRange;
import de.invesdwin.util.time.fdate.FDate;
import ezdb.serde.Serde;

@ThreadSafe
public abstract class ALiveSegmentedTimeSeriesDB<K, V> implements ITimeSeriesDB<K, V> {

    private final HistoricalSegmentTable historicalSegmentTable;
    private final ALoadingCache<K, ReadWriteLock> key_tableLock = new ALoadingCache<K, ReadWriteLock>() {
        @Override
        protected ReadWriteLock loadValue(final K key) {
            return new ReentrantReadWriteLock();
        }
    };
    private final AHistoricalCache<TimeRange> segmentFinder;
    private final ALoadingCache<K, LiveSegmentedTimeSeriesStorageCache<K, V>> key_lookupTableCache;

    public ALiveSegmentedTimeSeriesDB(final String name, final AHistoricalCache<TimeRange> segmentFinder) {
        this.historicalSegmentTable = new HistoricalSegmentTable(name, segmentFinder);
        this.segmentFinder = segmentFinder;
        this.key_lookupTableCache = new ALoadingCache<K, LiveSegmentedTimeSeriesStorageCache<K, V>>() {
            @Override
            protected LiveSegmentedTimeSeriesStorageCache<K, V> loadValue(final K key) {
                return new LiveSegmentedTimeSeriesStorageCache<K, V>(historicalSegmentTable, key, segmentFinder);
            }

        };
    }

    protected abstract ICloseableIterable<? extends V> downloadSegmentElements(K key, FDate from, FDate to);

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
        return historicalSegmentTable.getStorage();
    }

    protected abstract Integer newFixedLength();

    protected abstract Serde<V> newValueSerde();

    protected abstract FDate extractTime(V value);

    protected abstract FDate extractEndTime(V value);

    protected abstract String hashKeyToString(K key);

    protected abstract FDate getFirstAvailableHistoricalSegmentFrom(K key);

    protected abstract FDate getLastAvailableHistoricalSegmentTo(K key);

    protected final class HistoricalSegmentTable extends ASegmentedTimeSeriesDB<K, V> {
        private HistoricalSegmentTable(final String name, final AHistoricalCache<TimeRange> segmentFinder) {
            super(name, segmentFinder);
        }

        @Override
        protected Integer newFixedLength() {
            return ALiveSegmentedTimeSeriesDB.this.newFixedLength();
        }

        @Override
        protected Serde<V> newValueSerde() {
            return ALiveSegmentedTimeSeriesDB.this.newValueSerde();
        }

        @Override
        public FDate extractTime(final V value) {
            return ALiveSegmentedTimeSeriesDB.this.extractTime(value);
        }

        @Override
        public FDate extractEndTime(final V value) {
            return ALiveSegmentedTimeSeriesDB.this.extractEndTime(value);
        }

        @Override
        public String hashKeyToString(final K key) {
            return ALiveSegmentedTimeSeriesDB.this.hashKeyToString(key);
        }

        @Override
        protected SegmentedTimeSeriesStorage newStorage(final File directory) {
            return ALiveSegmentedTimeSeriesDB.this.newStorage(directory);
        }

        @Override
        protected void deleteCorruptedStorage(final File directory) {
            ALiveSegmentedTimeSeriesDB.this.deleteCorruptedStorage(directory);
        }

        @Override
        public synchronized SegmentedTimeSeriesStorage getStorage() {
            return super.getStorage();
        }

        @Override
        public ASegmentedTimeSeriesDB<K, V>.SegmentedTable getSegmentedTable() {
            return super.getSegmentedTable();
        }

        @Override
        protected File getBaseDirectory() {
            return ALiveSegmentedTimeSeriesDB.this.getBaseDirectory();
        }

        @Override
        protected ICloseableIterable<? extends V> downloadSegmentElements(final K key, final FDate from,
                final FDate to) {
            return ALiveSegmentedTimeSeriesDB.this.downloadSegmentElements(key, from, to);
        }

        @Override
        public FDate getFirstAvailableHistoricalSegmentFrom(final K key) {
            return ALiveSegmentedTimeSeriesDB.this.getFirstAvailableHistoricalSegmentFrom(key);
        }

        @Override
        public FDate getLastAvailableHistoricalSegmentTo(final K key) {
            return ALiveSegmentedTimeSeriesDB.this.getLastAvailableHistoricalSegmentTo(key);
        }

        @Override
        public ASegmentedTimeSeriesStorageCache<K, V> getLookupTableCache(final K key) {
            return super.getLookupTableCache(key);
        }

    }

    @Override
    public synchronized void close() throws IOException {
        historicalSegmentTable.close();
        key_lookupTableCache.clear();
        key_tableLock.clear();
    }

    @Override
    public File getDirectory() {
        return historicalSegmentTable.getDirectory();
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
            getLookupTableCache(key).deleteAll();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public String getName() {
        return historicalSegmentTable.getName();
    }

    private LiveSegmentedTimeSeriesStorageCache<K, V> getLookupTableCache(final K key) {
        return key_lookupTableCache.get(key);
    }

    @Override
    public ICloseableIterable<V> rangeValues(final K key, final FDate from, final FDate to) {
        return new ICloseableIterable<V>() {

            @Override
            public ICloseableIterator<V> iterator() {
                return new ACloseableIterator<V>() {

                    private final Lock readLock = getTableLock(key).readLock();
                    private ICloseableIterator<V> readRangeValues;

                    private ICloseableIterator<V> getReadRangeValues() {
                        if (readRangeValues == null) {
                            readLock.lock();
                            readRangeValues = getLookupTableCache(key).readRangeValues(from, to).iterator();
                            if (readRangeValues instanceof EmptyCloseableIterator) {
                                readLock.unlock();
                            }
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
        };
    }

    @Override
    public ICloseableIterable<V> rangeReverseValues(final K key, final FDate from, final FDate to) {
        return new ICloseableIterable<V>() {

            @Override
            public ICloseableIterator<V> iterator() {
                return new ACloseableIterator<V>() {

                    private final Lock readLock = getTableLock(key).readLock();
                    private ICloseableIterator<V> readRangeValues;

                    private ICloseableIterator<V> getReadRangeValues() {
                        if (readRangeValues == null) {
                            readLock.lock();
                            readRangeValues = getLookupTableCache(key).readRangeValuesReverse(from, to).iterator();
                            if (readRangeValues instanceof EmptyCloseableIterator) {
                                readLock.unlock();
                            }
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

    protected File getBaseDirectory() {
        return ATimeSeriesDB.getDefaultBaseDirectory();
    }

    public void putNextLiveValue(final K key, final V nextLiveValue) {
        final Lock writeLock = getTableLock(key).readLock();
        writeLock.lock();
        try {
            getLookupTableCache(key).putNextLiveValue(nextLiveValue);
        } finally {
            writeLock.unlock();
        }
    }
}
