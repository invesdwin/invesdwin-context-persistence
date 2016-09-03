package de.invesdwin.context.persistence.leveldb.timeseries;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Function;

import de.invesdwin.context.integration.retry.Retry;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.persistence.leveldb.serde.lazy.ILazySerdeValue;
import de.invesdwin.context.persistence.leveldb.serde.lazy.ILazySerdeValueSerde;
import de.invesdwin.context.persistence.leveldb.serde.lazy.LocalLazySerdeValueSerde;
import de.invesdwin.context.persistence.leveldb.timeseries.internal.TimeSeriesFileLookupTableCache;
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
    private final ILazySerdeValueSerde<V> localValueSerde;
    private final ALoadingCache<K, TimeSeriesFileLookupTableCache<K, V>> key_lookupTableCache;
    private final ALoadingCache<K, ReadWriteLock> key_tableLock = new ALoadingCache<K, ReadWriteLock>() {
        @Override
        protected ReadWriteLock loadValue(final K key) {
            return new ReentrantReadWriteLock();
        }
    };;

    public ATimeSeriesDB(final String name) {
        this.name = name;
        this.localValueSerde = new LocalLazySerdeValueSerde<V>(newValueSerde());
        this.fixedLength = newFixedLength();
        this.key_lookupTableCache = new ALoadingCache<K, TimeSeriesFileLookupTableCache<K, V>>() {
            @Override
            protected TimeSeriesFileLookupTableCache<K, V> loadValue(final K key) {
                return new TimeSeriesFileLookupTableCache<K, V>(getDatabaseName(key), localValueSerde.getDelegate(),
                        fixedLength, new Function<ILazySerdeValue<? extends V>, FDate>() {
                            @Override
                            public FDate apply(final ILazySerdeValue<? extends V> input) {
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

    protected abstract FDate extractTime(ILazySerdeValue<? extends V> value);

    public ICloseableIterator<? extends V> rangeValues(final K key, final FDate from, final FDate to) {
        return ILazySerdeValue.unlazy(rangeValues(key, from, to, localValueSerde));
    }

    public ICloseableIterator<ILazySerdeValue<? extends V>> rangeValues(final K key, final FDate from, final FDate to,
            final ILazySerdeValueSerde<V> valueSerdeOverride) {
        return new ACloseableIterator<ILazySerdeValue<? extends V>>() {

            private ICloseableIterator<ILazySerdeValue<? extends V>> readRangeValues;

            private ICloseableIterator<ILazySerdeValue<? extends V>> getReadRangeValues() {
                if (readRangeValues == null) {
                    getTableLock(key).readLock().lock();
                    readRangeValues = getLookupTableCache(key).readRangeValues(from, to, valueSerdeOverride);
                }
                return readRangeValues;
            }

            @Override
            public boolean innerHasNext() {
                return getReadRangeValues().hasNext();
            }

            @Override
            public ILazySerdeValue<? extends V> innerNext() {
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
                readRangeValues = new EmptyCloseableIterator<ILazySerdeValue<? extends V>>();
            }
        };
    }

    public ILazySerdeValue<? extends V> getLatestValue(final K key, final FDate date,
            final ILazySerdeValueSerde<V> valueSerdeOverride) {
        getTableLock(key).readLock().lock();
        try {
            if (date.isBeforeOrEqual(FDate.MIN_DATE)) {
                return getLookupTableCache(key).getFirstValue(valueSerdeOverride);
            } else if (date.isAfterOrEqual(FDate.MAX_DATE)) {
                return getLookupTableCache(key).getLastValue(valueSerdeOverride);
            } else {
                return getLookupTableCache(key).getLatestValue(date, valueSerdeOverride);
            }
        } finally {
            getTableLock(key).readLock().unlock();
        }
    }

    public V getLatestValue(final K key, final FDate date) {
        return ILazySerdeValue.unlazy(getLatestValue(key, date, localValueSerde));
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

    protected abstract String getDatabaseName(final K key);

    public ILazySerdeValueSerde<V> getLocalValueSerde() {
        return localValueSerde;
    }

}
