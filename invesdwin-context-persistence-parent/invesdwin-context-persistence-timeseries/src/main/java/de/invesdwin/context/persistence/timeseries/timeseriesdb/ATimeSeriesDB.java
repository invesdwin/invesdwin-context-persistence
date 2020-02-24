package de.invesdwin.context.persistence.timeseries.timeseriesdb;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Function;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.retry.Retry;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.storage.CorruptedTimeSeriesStorageException;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.storage.TimeSeriesStorage;
import de.invesdwin.util.collections.iterable.ACloseableIterator;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.Locks;
import de.invesdwin.util.concurrent.lock.readwrite.IReadWriteLock;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.description.TextDescription;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.time.fdate.FDate;
import ezdb.serde.Serde;

@ThreadSafe
public abstract class ATimeSeriesDB<K, V> implements ITimeSeriesDB<K, V> {

    private final String name;
    private final Serde<V> valueSerde;
    private final Integer fixedLength;
    private final File directory;
    private final ALoadingCache<K, TimeSeriesStorageCache<K, V>> key_lookupTableCache;
    private final ALoadingCache<K, IReadWriteLock> key_tableLock = new ALoadingCache<K, IReadWriteLock>() {
        @Override
        protected IReadWriteLock loadValue(final K key) {
            return Locks.newReentrantReadWriteLock(
                    ATimeSeriesDB.class.getSimpleName() + "_" + getName() + "_" + hashKeyToString(key) + "_tableLock");
        }

        @Override
        protected boolean isHighConcurrency() {
            return true;
        }
    };
    private final Object storageLock = new Object();
    @GuardedBy("storageLock")
    private TimeSeriesStorage storage;

    public ATimeSeriesDB(final String name) {
        this.name = name;
        this.valueSerde = newValueSerde();
        this.fixedLength = newValueFixedLength();
        this.directory = new File(getBaseDirectory(), ATimeSeriesDB.class.getSimpleName() + "/" + getName());
        this.key_lookupTableCache = new ALoadingCache<K, TimeSeriesStorageCache<K, V>>() {
            @Override
            protected TimeSeriesStorageCache<K, V> loadValue(final K key) {
                final String hashKey = hashKeyToString(key);
                return new TimeSeriesStorageCache<K, V>(getStorage(), hashKey, valueSerde, fixedLength,
                        new Function<V, FDate>() {
                            @Override
                            public FDate apply(final V input) {
                                return extractEndTime(input);
                            }
                        });
            }

            @Override
            protected boolean isHighConcurrency() {
                return true;
            }

        };
    }

    protected TimeSeriesStorage getStorage() {
        synchronized (storageLock) {
            if (storage == null) {
                storage = corruptionHandlingNewStorage();
            }
            return storage;
        }
    }

    private TimeSeriesStorage corruptionHandlingNewStorage() {
        try {
            return newStorage(directory, newValueFixedLength());
        } catch (final Throwable t) {
            if (Throwables.isCausedByType(t, CorruptedTimeSeriesStorageException.class)) {
                Err.process(new RuntimeException("Resetting " + ATimeSeriesDB.class.getSimpleName() + " ["
                        + getDirectory() + "] because the storage has been corrupted"));
                deleteCorruptedStorage(directory);
                return newStorage(directory, newValueFixedLength());
            } else {
                throw Throwables.propagate(t);
            }
        }
    }

    protected void deleteCorruptedStorage(final File directory) {
        Files.deleteQuietly(directory);
    }

    @Override
    public File getDirectory() {
        return directory;
    }

    public File getDataDirectory(final K key) {
        return getLookupTableCache(key).newDataDirectory();
    }

    protected TimeSeriesStorage newStorage(final File directory, final Integer valueFixedLength) {
        return new TimeSeriesStorage(directory, valueFixedLength);
    }

    protected File getBaseDirectory() {
        return getDefaultBaseDirectory();
    }

    public static File getDefaultBaseDirectory() {
        return ContextProperties.getHomeDirectory();
    }

    protected abstract Integer newValueFixedLength();

    public Integer getFixedLength() {
        return fixedLength;
    }

    @Override
    public IReadWriteLock getTableLock(final K key) {
        return key_tableLock.get(key);
    }

    protected abstract Serde<V> newValueSerde();

    protected abstract FDate extractEndTime(V value);

    @Override
    public ICloseableIterable<V> rangeValues(final K key, final FDate from, final FDate to) {
        return new RangeValues(key, from, to);
    }

    @Override
    public ICloseableIterable<V> rangeReverseValues(final K key, final FDate from, final FDate to) {
        return new RangeReverseValues(key, from, to);
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
    public FDate getLatestValueKey(final K key, final FDate date) {
        final V value = getLatestValue(key, date);
        if (value == null) {
            return null;
        } else {
            return extractEndTime(value);
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
    public FDate getPreviousValueKey(final K key, final FDate date, final int shiftBackUnits) {
        final V value = getPreviousValue(key, date, shiftBackUnits);
        if (value == null) {
            return null;
        } else {
            return extractEndTime(value);
        }
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

    @Override
    public FDate getNextValueKey(final K key, final FDate date, final int shiftForwardUnits) {
        final V value = getNextValue(key, date, shiftForwardUnits);
        if (value == null) {
            return null;
        } else {
            return extractEndTime(value);
        }
    }

    @Override
    @Retry(fixedBackOffMillis = 1000)
    public void deleteRange(final K key) {
        final ILock writeLock = getTableLock(key).writeLock();
        try {
            if (!writeLock.tryLock(1, TimeUnit.MINUTES)) {
                throw Locks.getLockTrace()
                        .handleLockException(writeLock.getName(),
                                new RetryLaterRuntimeException("Write lock could not be acquired for table [" + name
                                        + "] and key [" + key + "]. Please ensure all iterators are closed!"));
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
        return name;
    }

    public TimeSeriesStorageCache<K, V> getLookupTableCache(final K key) {
        return key_lookupTableCache.get(key);
    }

    public Serde<V> getValueSerde() {
        return valueSerde;
    }

    public final String hashKeyToString(final K key) {
        return Files.normalizeFilename(innerHashKeyToString(key));
    }

    protected abstract String innerHashKeyToString(K key);

    @Override
    public void close() {
        synchronized (storageLock) {
            if (storage != null) {
                storage.close();
                storage = null;
            }
        }
        key_lookupTableCache.clear();
        key_tableLock.clear();
    }

    private final class RangeReverseValues implements ICloseableIterable<V> {
        private final K key;
        private final FDate from;
        private final FDate to;

        private RangeReverseValues(final K key, final FDate from, final FDate to) {
            this.key = key;
            this.from = from;
            this.to = to;
        }

        @Override
        public ICloseableIterator<V> iterator() {
            return new ACloseableIterator<V>(new TextDescription("%s: %s(%s, %s, %s)",
                    ATimeSeriesDB.class.getSimpleName(), RangeReverseValues.class.getSimpleName(), key, from, to)) {

                private final RangeValuesFinalizer<V> finalizer = new RangeValuesFinalizer<>();

                {
                    this.finalizer.register(this);
                }

                private ICloseableIterator<V> getReadRangeValues() {
                    if (finalizer.readRangeValues == null) {
                        finalizer.readRangeValues = getLookupTableCache(key).readRangeValuesReverse(from, to,
                                getTableLock(key).readLock(), null);
                    }
                    return finalizer.readRangeValues;
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
                protected void innerClose() {
                    finalizer.close();
                }
            };
        }

    }

    private final class RangeValues implements ICloseableIterable<V> {
        private final K key;
        private final FDate from;
        private final FDate to;

        private RangeValues(final K key, final FDate from, final FDate to) {
            this.key = key;
            this.from = from;
            this.to = to;
        }

        @Override
        public ICloseableIterator<V> iterator() {
            return new ACloseableIterator<V>(new TextDescription("%s: %s(%s, %s, %s)",
                    ATimeSeriesDB.class.getSimpleName(), RangeValues.class.getSimpleName(), key, from, to)) {

                private final RangeValuesFinalizer<V> finalizer = new RangeValuesFinalizer<>();

                {
                    this.finalizer.register(this);
                }

                private ICloseableIterator<V> getReadRangeValues() {
                    if (finalizer.readRangeValues == null) {
                        finalizer.readRangeValues = getLookupTableCache(key).readRangeValues(from, to,
                                getTableLock(key).readLock(), null);
                    }
                    return finalizer.readRangeValues;
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
                protected void innerClose() {
                    finalizer.close();
                }

            };
        }
    }

    private static final class RangeValuesFinalizer<_V> extends AFinalizer {

        private ICloseableIterator<_V> readRangeValues;

        @Override
        protected void clean() {
            if (readRangeValues != null) {
                readRangeValues.close();
            }
            readRangeValues = EmptyCloseableIterator.getInstance();
        }

        @Override
        protected boolean isCleaned() {
            return readRangeValues instanceof EmptyCloseableIterator;
        }

        @Override
        public boolean isThreadLocal() {
            return true;
        }

    }

}
