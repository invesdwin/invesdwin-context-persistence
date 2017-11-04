package de.invesdwin.context.persistence.leveldb.timeseries;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;

import com.google.common.base.Function;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.retry.Retry;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.persistence.leveldb.timeseries.storage.CorruptedTimeSeriesStorageException;
import de.invesdwin.context.persistence.leveldb.timeseries.storage.TimeSeriesStorage;
import de.invesdwin.util.collections.iterable.ACloseableIterator;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.time.fdate.FDate;
import ezdb.serde.Serde;

@ThreadSafe
public abstract class ATimeSeriesDB<K, V> {

    private final String name;
    private final Integer fixedLength;
    private final Serde<V> valueSerde;
    private final ALoadingCache<K, TimeSeriesStorageCache<K, V>> key_lookupTableCache;
    private final ALoadingCache<K, ReadWriteLock> key_tableLock = new ALoadingCache<K, ReadWriteLock>() {
        @Override
        protected ReadWriteLock loadValue(final K key) {
            return new ReentrantReadWriteLock();
        }
    };
    private TimeSeriesStorage storage;

    public ATimeSeriesDB(final String name) {
        this.name = name;
        final File directory = new File(getBaseDirectory(), getClass().getSimpleName() + "/" + getName());
        this.storage = corruptionHandlingNewStorage(directory);
        this.valueSerde = newValueSerde();
        this.fixedLength = newFixedLength();
        this.key_lookupTableCache = new ALoadingCache<K, TimeSeriesStorageCache<K, V>>() {
            @Override
            protected TimeSeriesStorageCache<K, V> loadValue(final K key) {
                final String hashKey = getName() + "_" + hashKeyToString(key);
                return new TimeSeriesStorageCache<K, V>(storage, hashKey, valueSerde, fixedLength,
                        new Function<V, FDate>() {
                            @Override
                            public FDate apply(final V input) {
                                return extractTime(input);
                            }
                        });
            }

        };
    }

    private TimeSeriesStorage corruptionHandlingNewStorage(final File directory) {
        try {
            return newStorage(directory);
        } catch (final Throwable t) {
            if (Throwables.isCausedByType(t, CorruptedTimeSeriesStorageException.class)) {
                Err.process(new RuntimeException("Resetting " + ATimeSeriesDB.class.getSimpleName() + " ["
                        + getDirectory() + "] because the storage has been corrupted"));
                FileUtils.deleteQuietly(directory);
                return new TimeSeriesStorage(directory);
            } else {
                throw Throwables.propagate(t);
            }
        }
    }

    public File getDirectory() {
        return storage.getDirectory();
    }

    public File getDataDirectory(final K key) {
        return getLookupTableCache(key).getDataDirectory();
    }

    protected TimeSeriesStorage newStorage(final File directory) {
        return new TimeSeriesStorage(directory);
    }

    protected File getBaseDirectory() {
        return ContextProperties.getHomeDirectory();
    }

    protected abstract Integer newFixedLength();

    public final Integer getFixedLength() {
        return fixedLength;
    }

    public ReadWriteLock getTableLock(final K key) {
        return key_tableLock.get(key);
    }

    protected abstract Serde<V> newValueSerde();

    protected abstract FDate extractTime(V value);

    public ICloseableIterator<V> rangeValues(final K key, final FDate from, final FDate to) {

        return new ACloseableIterator<V>() {

            private ICloseableIterator<V> readRangeValues;

            private ICloseableIterator<V> getReadRangeValues() {
                if (readRangeValues == null) {
                    getTableLock(key).readLock().lock();
                    readRangeValues = getLookupTableCache(key).readRangeValues(from, to);
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
                    getTableLock(key).readLock().unlock();
                }
                readRangeValues = EmptyCloseableIterator.getInstance();
            }
        };
    }

    public V getLatestValue(final K key, final FDate date) {
        getTableLock(key).readLock().lock();
        try {
            if (date.isBeforeOrEqualTo(FDate.MIN_DATE)) {
                return getLookupTableCache(key).getFirstValue();
            } else if (date.isAfterOrEqualTo(FDate.MAX_DATE)) {
                return getLookupTableCache(key).getLastValue();
            } else {
                return getLookupTableCache(key).getLatestValue(date);
            }
        } finally {
            getTableLock(key).readLock().unlock();
        }
    }

    public FDate getLatestValueKey(final K key, final FDate date) {
        final V latestValue = getLatestValue(key, date);
        if (latestValue == null) {
            return null;
        } else {
            return extractTime(latestValue);
        }
    }

    public V getPreviousValue(final K key, final FDate date, final int shiftBackUnits) {
        getTableLock(key).readLock().lock();
        try {
            if (date.isBeforeOrEqualTo(FDate.MIN_DATE)) {
                return getLookupTableCache(key).getFirstValue();
            } else {
                return getLookupTableCache(key).getPreviousValue(date, shiftBackUnits);
            }
        } finally {
            getTableLock(key).readLock().unlock();
        }
    }

    public FDate getPreviousValueKey(final K key, final FDate date, final int shiftBackUnits) {
        final V latestValue = getPreviousValue(key, date, shiftBackUnits);
        if (latestValue == null) {
            return null;
        } else {
            return extractTime(latestValue);
        }
    }

    public boolean isEmptyOrInconsistent(final K key) {
        getTableLock(key).readLock().lock();
        try {
            return getLookupTableCache(key).isEmptyOrInconsistent();
        } finally {
            getTableLock(key).readLock().unlock();
        }
    }

    public V getNextValue(final K key, final FDate date, final int shiftForwardUnits) {
        getTableLock(key).readLock().lock();
        try {
            if (date.isAfterOrEqualTo(FDate.MAX_DATE)) {
                return getLookupTableCache(key).getLastValue();
            } else {
                return getLookupTableCache(key).getNextValue(date, shiftForwardUnits);
            }
        } finally {
            getTableLock(key).readLock().unlock();
        }
    }

    public FDate getNextValueKey(final K key, final FDate date, final int shiftForwardUnits) {
        final V nextValue = getNextValue(key, date, shiftForwardUnits);
        if (nextValue == null) {
            return null;
        } else {
            return extractTime(nextValue);
        }
    }

    @Retry
    public void deleteRange(final K key) {
        try {
            if (!getTableLock(key).writeLock().tryLock(1, TimeUnit.MINUTES)) {
                throw new RetryLaterRuntimeException("Write lock could not be acquired for table [" + name
                        + "] and key [" + key + "]. Please ensure all iterators are closed!");
            }
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            getLookupTableCache(key).deleteAll();
        } finally {
            getTableLock(key).writeLock().unlock();
        }
    }

    public String getName() {
        return name;
    }

    TimeSeriesStorageCache<K, V> getLookupTableCache(final K key) {
        return key_lookupTableCache.get(key);
    }

    protected Serde<V> getValueSerde() {
        return valueSerde;
    }

    protected abstract String hashKeyToString(K key);

}
