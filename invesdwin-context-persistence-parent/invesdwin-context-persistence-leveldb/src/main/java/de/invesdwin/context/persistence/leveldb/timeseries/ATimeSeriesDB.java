package de.invesdwin.context.persistence.leveldb.timeseries;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Function;

import de.invesdwin.context.integration.retry.Retry;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.util.collections.iterable.ACloseableIterator;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import de.invesdwin.util.time.fdate.FDate;
import ezdb.serde.Serde;

@ThreadSafe
public abstract class ATimeSeriesDB<K, V> {

    private final String name;
    private final Integer fixedLength;
    private final Serde<V> valueSerde;
    private final ALoadingCache<K, TimeSeriesFileLookupTableCache<K, V>> key_lookupTableCache;
    private final ALoadingCache<K, ReadWriteLock> key_tableLock = new ALoadingCache<K, ReadWriteLock>() {
        @Override
        protected ReadWriteLock loadValue(final K key) {
            return new ReentrantReadWriteLock();
        }
    };;

    public ATimeSeriesDB(final String name) {
        this.name = name;
        this.valueSerde = newValueSerde();
        this.fixedLength = newFixedLength();
        this.key_lookupTableCache = new ALoadingCache<K, TimeSeriesFileLookupTableCache<K, V>>() {
            @Override
            protected TimeSeriesFileLookupTableCache<K, V> loadValue(final K key) {
                return new TimeSeriesFileLookupTableCache<K, V>(getDatabaseName(key), valueSerde, fixedLength,
                        new Function<V, FDate>() {
                            @Override
                            public FDate apply(final V input) {
                                return extractTime(input);
                            }
                        });
            }
        };
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
            if (date.isBeforeOrEqual(FDate.MIN_DATE)) {
                return getLookupTableCache(key).getFirstValue();
            } else if (date.isAfterOrEqual(FDate.MAX_DATE)) {
                return getLookupTableCache(key).getLastValue();
            } else {
                return getLookupTableCache(key).getLatestValue(date);
            }
        } finally {
            getTableLock(key).readLock().unlock();
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

    public FDate getNextRangeKey(final K key, final FDate date) {
        throw new UnsupportedOperationException();
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

    TimeSeriesFileLookupTableCache<K, V> getLookupTableCache(final K key) {
        return key_lookupTableCache.get(key);
    }

    protected Serde<V> getValueSerde() {
        return valueSerde;
    }

    protected abstract String getDatabaseName(final K key);

}
