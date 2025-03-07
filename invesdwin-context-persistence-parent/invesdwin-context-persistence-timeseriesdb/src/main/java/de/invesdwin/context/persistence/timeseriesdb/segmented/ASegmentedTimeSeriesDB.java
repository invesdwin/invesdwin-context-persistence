package de.invesdwin.context.persistence.timeseriesdb.segmented;

import java.io.File;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.compression.lz4.LZ4Streams;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupMode;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.context.persistence.timeseriesdb.segmented.finder.ISegmentFinder;
import de.invesdwin.context.persistence.timeseriesdb.storage.TimeSeriesStorage;
import de.invesdwin.context.persistence.timeseriesdb.updater.ITimeSeriesUpdater;
import de.invesdwin.util.collections.iterable.ACloseableIterator;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import de.invesdwin.util.collections.loadingcache.ILoadingCache;
import de.invesdwin.util.concurrent.lambda.callable.AFastLazyCallable;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.Locks;
import de.invesdwin.util.concurrent.lock.readwrite.IReadWriteLock;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;

@ThreadSafe
public abstract class ASegmentedTimeSeriesDB<K, V> implements ISegmentedTimeSeriesDBInternals<K, V> {

    private final Supplier<ISerde<V>> valueSerde;
    private final Supplier<Integer> valueFixedLength;
    private final ICompressionFactory compressionFactory;
    private final TimeSeriesLookupMode lookupMode;
    private final SegmentedTable segmentedTable;
    private final ILoadingCache<K, IReadWriteLock> key_tableLock = new ALoadingCache<K, IReadWriteLock>() {
        @Override
        protected IReadWriteLock loadValue(final K key) {
            return Locks.newReentrantReadWriteLock(ASegmentedTimeSeriesDB.class.getSimpleName() + "_" + getName() + "_"
                    + hashKeyToString(key) + "_tableLock");
        }

        @Override
        protected boolean isHighConcurrency() {
            return true;
        }
    };
    private final ILoadingCache<K, ASegmentedTimeSeriesStorageCache<K, V>> key_segmentedLookupTableCache;

    public ASegmentedTimeSeriesDB(final String name) {
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
        this.segmentedTable = new SegmentedTable(name);
        this.key_segmentedLookupTableCache = new ALoadingCache<K, ASegmentedTimeSeriesStorageCache<K, V>>() {
            @Override
            protected ASegmentedTimeSeriesStorageCache<K, V> loadValue(final K key) {
                final String hashKey = hashKeyToString(key);
                return new ASegmentedTimeSeriesStorageCache<K, V>(segmentedTable, getStorage(), key, hashKey) {

                    @Override
                    protected FDate getLastAvailableSegmentTo(final K key, final FDate updateTo) {
                        return ASegmentedTimeSeriesDB.this.getLastAvailableHistoricalSegmentTo(key, updateTo);
                    }

                    @Override
                    protected FDate getFirstAvailableSegmentFrom(final K key) {
                        return ASegmentedTimeSeriesDB.this.getFirstAvailableHistoricalSegmentFrom(key);
                    }

                    @Override
                    protected ICloseableIterable<? extends V> downloadSegmentElements(
                            final SegmentedKey<K> segmentedKey) {
                        return ASegmentedTimeSeriesDB.this.downloadSegmentElements(segmentedKey);
                    }

                    @Override
                    protected ISegmentFinder getSegmentFinder(final K key) {
                        return ASegmentedTimeSeriesDB.this.getSegmentFinder(key);
                    }

                    @Override
                    protected String getElementsName() {
                        return ASegmentedTimeSeriesDB.this.getElementsName();
                    }

                    @Override
                    public void onSegmentCompleted(final SegmentedKey<K> segmentedKey,
                            final ICloseableIterable<V> segmentValues) {
                        super.onSegmentCompleted(segmentedKey, segmentValues);
                        ASegmentedTimeSeriesDB.this.onSegmentCompleted(segmentedKey, segmentValues);
                    }

                    @Override
                    protected ITimeSeriesUpdater<SegmentedKey<K>, V> newSegmentUpdaterOverride(
                            final SegmentedKey<K> segmentedKey,
                            final ASegmentedTimeSeriesDB<K, V>.SegmentedTable segmentedTable,
                            final Function<SegmentedKey<K>, ICloseableIterable<? extends V>> source) {
                        return ASegmentedTimeSeriesDB.this.newSegmentUpdaterOverride(segmentedKey, segmentedTable,
                                source);
                    }

                };
            }

            @Override
            protected boolean isHighConcurrency() {
                return true;
            }

        };
    }

    @Override
    public ISerde<V> getValueSerde() {
        return valueSerde.get();
    }

    @Override
    public Integer getValueFixedLength() {
        return valueFixedLength.get();
    }

    @Override
    public ICompressionFactory getCompressionFactory() {
        return compressionFactory;
    }

    @Override
    public TimeSeriesLookupMode getLookupMode() {
        return lookupMode;
    }

    protected ITimeSeriesUpdater<SegmentedKey<K>, V> newSegmentUpdaterOverride(final SegmentedKey<K> segmentedKey,
            final ASegmentedTimeSeriesDB<K, V>.SegmentedTable segmentedTable,
            final Function<SegmentedKey<K>, ICloseableIterable<? extends V>> source) {
        return null;
    }

    protected void onSegmentCompleted(final SegmentedKey<K> segmentedKey, final ICloseableIterable<V> segmentValues) {}

    protected abstract String getElementsName();

    protected abstract ICloseableIterable<? extends V> downloadSegmentElements(SegmentedKey<K> segmentedKey);

    protected SegmentedTimeSeriesStorage newStorage(final File directory, final Integer valueFixedLength,
            final ICompressionFactory compressionFactory) {
        return new SegmentedTimeSeriesStorage(directory, valueFixedLength, compressionFactory);
    }

    @Override
    public SegmentedTable getSegmentedTable() {
        return segmentedTable;
    }

    protected void deleteCorruptedStorage(final File directory) {
        directory.delete();
    }

    public abstract ISegmentFinder getSegmentFinder(K key);

    @Override
    public SegmentedTimeSeriesStorage getStorage() {
        return (SegmentedTimeSeriesStorage) segmentedTable.getStorage();
    }

    protected abstract Integer newValueFixedLength();

    protected abstract ISerde<V> newValueSerde();

    protected ICompressionFactory newCompressionFactory() {
        return LZ4Streams.getDefaultCompressionFactory();
    }

    protected TimeSeriesLookupMode newLookupMode() {
        return TimeSeriesLookupMode.DEFAULT;
    }

    @Override
    public abstract FDate extractStartTime(V value);

    @Override
    public abstract FDate extractEndTime(V value);

    @Override
    public final String hashKeyToString(final K key) {
        return Files.normalizeFilename(innerHashKeyToString(key));
    }

    protected abstract String innerHashKeyToString(K key);

    @Override
    public final String hashKeyToString(final SegmentedKey<K> key) {
        return ASegmentedTimeSeriesDB.this.hashKeyToString(key.getKey()) + "/"
                + key.getSegment().getFrom().toString(FDate.FORMAT_UNDERSCORE_DATE_TIME_MS) + "-"
                + key.getSegment().getTo().toString(FDate.FORMAT_UNDERSCORE_DATE_TIME_MS);
    }

    public abstract FDate getFirstAvailableHistoricalSegmentFrom(K key);

    public abstract FDate getLastAvailableHistoricalSegmentTo(K key, FDate updateTo);

    @Override
    public synchronized void close() {
        for (final ASegmentedTimeSeriesStorageCache<?, ?> cache : key_segmentedLookupTableCache.values()) {
            cache.close();
        }
        key_segmentedLookupTableCache.clear();
        key_tableLock.clear();
        segmentedTable.close();
    }

    @Override
    public File getDirectory() {
        return segmentedTable.getDirectory();
    }

    @Override
    public IReadWriteLock getTableLock(final K key) {
        return key_tableLock.get(key);
    }

    @Override
    public boolean isEmptyOrInconsistent(final K key) {
        final ILock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            return getSegmentedLookupTableCache(key).isEmptyOrInconsistent();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void deleteRange(final K key) {
        final ILock writeLock = getTableLock(key).writeLock();
        try {
            if (!writeLock.tryLock(TimeSeriesProperties.ACQUIRE_WRITE_LOCK_TIMEOUT)) {
                throw Locks.getLockTrace()
                        .handleLockException(writeLock.getName(),
                                new RetryLaterRuntimeException(
                                        "Write lock could not be acquired for table [" + getName() + "] and key [" + key
                                                + "]. Please ensure all iterators are closed!"));
            }
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            getSegmentedLookupTableCache(key).deleteAll();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public String getName() {
        return segmentedTable.getName();
    }

    @Override
    public ASegmentedTimeSeriesStorageCache<K, V> getSegmentedLookupTableCache(final K key) {
        return key_segmentedLookupTableCache.get(key);
    }

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
                return getSegmentedLookupTableCache(key).getFirstValue();
            } else if (date.isAfterOrEqualTo(FDates.MAX_DATE)) {
                return getSegmentedLookupTableCache(key).getLastValue();
            } else {
                return getSegmentedLookupTableCache(key).getLatestValue(date);
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
    public V getLatestValue(final K key, final long index) {
        final ILock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            final ASegmentedTimeSeriesStorageCache<K, V> lookupTableCache = getSegmentedLookupTableCache(key);
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
    public long getLatestValueIndex(final K key, final FDate date) {
        final ILock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            if (date.isBeforeOrEqualTo(FDates.MIN_DATE)) {
                if (getSegmentedLookupTableCache(key).size() == 0) {
                    return -1L;
                } else {
                    return 0L;
                }
            } else if (date.isAfterOrEqualTo(FDates.MAX_DATE)) {
                return getSegmentedLookupTableCache(key).size() - 1;
            } else {
                return getSegmentedLookupTableCache(key).getLatestValueIndex(date);
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long size(final K key) {
        final ILock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            return getSegmentedLookupTableCache(key).size();
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
    public V getPreviousValue(final K key, final FDate date, final int shiftBackUnits) {
        final ILock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            if (date == null) {
                return null;
            } else if (date.isBeforeOrEqualTo(FDates.MIN_DATE)) {
                return getSegmentedLookupTableCache(key).getFirstValue();
            } else {
                return getSegmentedLookupTableCache(key).getPreviousValue(date, shiftBackUnits);
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
    public V getNextValue(final K key, final FDate date, final int shiftForwardUnits) {
        final ILock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            if (date.isAfterOrEqualTo(FDates.MAX_DATE)) {
                return getSegmentedLookupTableCache(key).getLastValue();
            } else {
                return getSegmentedLookupTableCache(key).getNextValue(date, shiftForwardUnits);
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public File getBaseDirectory() {
        return ATimeSeriesDB.getDefaultBaseDirectory();
    }

    protected String getStorageName(final String name) {
        return ATimeSeriesDB.getDefaultStorageName(name);
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
            return new ACloseableIterator<V>(
                    new TextDescription("%s: %s(%s, %s, %s)", ASegmentedTimeSeriesDB.class.getSimpleName(),
                            RangeReverseValues.class.getSimpleName(), key, from, to)) {

                private final RangeValuesFinalizer<V> finalizer = new RangeValuesFinalizer<>();

                {
                    this.finalizer.register(this);
                }

                private ICloseableIterator<V> getReadRangeValues() {
                    if (finalizer.readRangeValues == null) {
                        finalizer.readRangeValues = getSegmentedLookupTableCache(key)
                                .readRangeValuesReverse(from, to, getTableLock(key).readLock(), null)
                                .iterator();
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
                    ASegmentedTimeSeriesDB.class.getSimpleName(), RangeValues.class.getSimpleName(), key, from, to)) {

                private final RangeValuesFinalizer<V> finalizer = new RangeValuesFinalizer<>();

                {
                    this.finalizer.register(this);
                }

                private ICloseableIterator<V> getReadRangeValues() {
                    if (finalizer.readRangeValues == null) {
                        finalizer.readRangeValues = getSegmentedLookupTableCache(key)
                                .readRangeValues(from, to, getTableLock(key).readLock(), null)
                                .iterator();
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

    public final class SegmentedTable extends ATimeSeriesDB<SegmentedKey<K>, V> {
        private SegmentedTable(final String name) {
            super(name);
        }

        @Override
        protected Integer newValueFixedLength() {
            return ASegmentedTimeSeriesDB.this.getValueFixedLength();
        }

        @Override
        protected ICompressionFactory newCompressionFactory() {
            return ASegmentedTimeSeriesDB.this.getCompressionFactory();
        }

        @Override
        protected TimeSeriesLookupMode newLookupMode() {
            return ASegmentedTimeSeriesDB.this.getLookupMode();
        }

        @Override
        protected ISerde<V> newValueSerde() {
            return ASegmentedTimeSeriesDB.this.getValueSerde();
        }

        @Override
        public FDate extractStartTime(final V value) {
            return ASegmentedTimeSeriesDB.this.extractStartTime(value);
        }

        @Override
        public FDate extractEndTime(final V value) {
            return ASegmentedTimeSeriesDB.this.extractEndTime(value);
        }

        @Override
        protected String innerHashKeyToString(final SegmentedKey<K> key) {
            return ASegmentedTimeSeriesDB.this.hashKeyToString(key);
        }

        @Override
        protected TimeSeriesStorage newStorage(final File directory, final Integer valueFixedLength,
                final ICompressionFactory compressionFactory) {
            return ASegmentedTimeSeriesDB.this.newStorage(directory, valueFixedLength, compressionFactory);
        }

        @Override
        protected void deleteCorruptedStorage(final File directory) {
            ASegmentedTimeSeriesDB.this.deleteCorruptedStorage(directory);
        }

        @Override
        protected String getStorageName(final String name) {
            return ASegmentedTimeSeriesDB.this.getStorageName(name);
        }

        @Override
        public File getBaseDirectory() {
            return ASegmentedTimeSeriesDB.this.getBaseDirectory();
        }

    }

}
