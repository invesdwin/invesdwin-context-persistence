package de.invesdwin.context.persistence.timeseriesdb;

import java.io.File;
import java.util.function.Supplier;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.compression.lz4.LZ4Streams;
import de.invesdwin.context.integration.persistentmap.CorruptedStorageException;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.persistence.timeseriesdb.storage.TimeSeriesStorage;
import de.invesdwin.util.collections.iterable.ACloseableIterator;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import de.invesdwin.util.collections.loadingcache.caffeine.ACaffeineLoadingCache;
import de.invesdwin.util.concurrent.lambda.callable.AFastLazyCallable;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.Locks;
import de.invesdwin.util.concurrent.lock.readwrite.IReadWriteLock;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public abstract class ATimeSeriesDB<K, V> implements ITimeSeriesDBInternals<K, V> {

    private final String name;
    private final Supplier<ISerde<V>> valueSerde;
    private final Supplier<Integer> valueFixedLength;
    private final ICompressionFactory compressionFactory;
    private final TimeSeriesLookupMode lookupMode;
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
        this.valueSerde = new AFastLazyCallable<ISerde<V>>() {
            @Override
            protected ISerde<V> innerCall() {
                return newValueSerde();
            }
        };
        this.valueFixedLength = new AFastLazyCallable<Integer>() {
            @Override
            protected Integer innerCall() {
                return newValueFixedLength();
            }
        };
        this.compressionFactory = newCompressionFactory();
        this.lookupMode = newLookupMode();
        final File baseDirectory = getBaseDirectory();
        if (Objects.equals(baseDirectory.getAbsolutePath(), new File(".").getAbsolutePath())) {
            throw new IllegalStateException(
                    "Should not use current working directory as base directory: " + baseDirectory);
        }
        this.directory = new File(baseDirectory, getStorageName(Files.normalizePath(getName())));
        this.key_lookupTableCache = new ACaffeineLoadingCache<K, TimeSeriesStorageCache<K, V>>() {
            @Override
            protected TimeSeriesStorageCache<K, V> loadValue(final K key) {
                final String hashKey = hashKeyToString(key);
                return new TimeSeriesStorageCache<K, V>(getStorage(), hashKey, getValueSerde(), getValueFixedLength(),
                        input -> extractEndTime(input), getLookupMode());
            }

            @Override
            protected boolean isHighConcurrency() {
                return true;
            }

            @Override
            protected Boolean getSoftValues() {
                return true;
            }

            @Override
            protected Integer getInitialMaximumSize() {
                return TimeSeriesStorageCache.MAXIMUM_SIZE;
            }

            @Override
            protected Duration getExpireAfterAccess() {
                return TimeSeriesProperties.STORAGE_CACHE_EVICTION_TIMEOUT;
            }

        };
    }

    public TimeSeriesStorage getStorage() {
        synchronized (storageLock) {
            if (storage == null) {
                storage = corruptionHandlingNewStorage();
            }
            return storage;
        }
    }

    private TimeSeriesStorage corruptionHandlingNewStorage() {
        try {
            return newStorage(directory, getValueFixedLength(), compressionFactory);
        } catch (final Throwable t) {
            if (Throwables.isCausedByType(t, CorruptedStorageException.class)) {
                Err.process(new RuntimeException("Resetting " + ATimeSeriesDB.class.getSimpleName() + " ["
                        + getDirectory() + "] because the storage has been corrupted"));
                deleteCorruptedStorage(directory);
                return newStorage(directory, getValueFixedLength(), compressionFactory);
            } else {
                throw Throwables.propagate(t);
            }
        }
    }

    protected void deleteCorruptedStorage(final File directory) {
        Files.deleteNative(directory);
    }

    @Override
    public File getDirectory() {
        return directory;
    }

    public File getDataDirectory(final K key) {
        return getLookupTableCache(key).newDataDirectory();
    }

    protected TimeSeriesStorage newStorage(final File directory, final Integer valueFixedLength,
            final ICompressionFactory compressionFactory) {
        return new TimeSeriesStorage(directory, valueFixedLength, compressionFactory);
    }

    @Override
    public File getBaseDirectory() {
        return getDefaultBaseDirectory();
    }

    protected String getStorageName(final String name) {
        return getDefaultStorageName(name);
    }

    public static String getDefaultStorageName(final String name) {
        return ATimeSeriesDB.class.getSimpleName() + "/" + name;
    }

    public static File getDefaultBaseDirectory() {
        return ContextProperties.getHomeDataDirectory();
    }

    protected abstract Integer newValueFixedLength();

    @Override
    public Integer getValueFixedLength() {
        return valueFixedLength.get();
    }

    @Override
    public IReadWriteLock getTableLock(final K key) {
        return key_tableLock.get(key);
    }

    protected abstract ISerde<V> newValueSerde();

    protected ICompressionFactory newCompressionFactory() {
        //        return DisabledCompressionFactory.INSTANCE; //use this to enable flyweight mmap access
        return LZ4Streams.getDefaultCompressionFactory();
    }

    protected TimeSeriesLookupMode newLookupMode() {
        return TimeSeriesLookupMode.DEFAULT;
    }

    @Override
    public abstract FDate extractEndTime(V value);

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
        final ILock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            if (date.isBeforeOrEqualTo(FDates.MIN_DATE)) {
                return getLookupTableCache(key).getFirstValue();
            } else if (date.isAfterOrEqualTo(FDates.MAX_DATE)) {
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
    public long getLatestValueIndex(final K key, final FDate date) {
        final ILock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            if (date.isBeforeOrEqualTo(FDates.MIN_DATE)) {
                if (getLookupTableCache(key).size() == 0) {
                    return -1L;
                } else {
                    return 0L;
                }
            } else if (date.isAfterOrEqualTo(FDates.MAX_DATE)) {
                return getLookupTableCache(key).size() - 1;
            } else {
                return getLookupTableCache(key).getLatestValueIndex(date);
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public V getLatestValue(final K key, final long index) {
        final ILock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            final TimeSeriesStorageCache<K, V> lookupTableCache = getLookupTableCache(key);
            if (index <= 0) {
                return lookupTableCache.getFirstValue();
            } else if (index >= lookupTableCache.size()) {
                return lookupTableCache.getLastValue();
            } else {
                return lookupTableCache.getLatestValue(index);
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public FDate getLatestValueKey(final K key, final long index) {
        final V value = getLatestValue(key, index);
        if (value == null) {
            return null;
        } else {
            return extractEndTime(value);
        }
    }

    @Override
    public long size(final K key) {
        final ILock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            return getLookupTableCache(key).size();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public V getPreviousValue(final K key, final FDate date, final int shiftBackUnits) {
        final ILock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            if (date == null) {
                return null;
            } else if (date.isBeforeOrEqualTo(FDates.MIN_DATE)) {
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
        final ILock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            return getLookupTableCache(key).isEmptyOrInconsistent();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public V getNextValue(final K key, final FDate date, final int shiftForwardUnits) {
        final ILock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            if (date.isAfterOrEqualTo(FDates.MAX_DATE)) {
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
    public void deleteRange(final K key) {
        final ILock writeLock = getTableLock(key).writeLock();
        try {
            if (!writeLock.tryLock(TimeSeriesProperties.ACQUIRE_WRITE_LOCK_TIMEOUT)) {
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

    @Override
    public TimeSeriesStorageCache<K, V> getLookupTableCache(final K key) {
        return key_lookupTableCache.get(key);
    }

    @Override
    public ISerde<V> getValueSerde() {
        return valueSerde.get();
    }

    @Override
    public TimeSeriesLookupMode getLookupMode() {
        return lookupMode;
    }

    @Override
    public ICompressionFactory getCompressionFactory() {
        return compressionFactory;
    }

    @Override
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
