package de.invesdwin.context.persistence.timeseriesdb.segmented.live;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.compression.lz4.LZ4Streams;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.ITimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.segmented.ASegmentedTimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.segmented.ASegmentedTimeSeriesStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentedTimeSeriesStorage;
import de.invesdwin.context.persistence.timeseriesdb.segmented.finder.ISegmentFinder;
import de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.context.persistence.timeseriesdb.updater.ITimeSeriesUpdater;
import de.invesdwin.util.collections.iterable.ACloseableIterator;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.Locks;
import de.invesdwin.util.concurrent.lock.readwrite.IReadWriteLock;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.description.TextDescription;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.time.date.FDate;

@ThreadSafe
public abstract class ALiveSegmentedTimeSeriesDB<K, V> implements ITimeSeriesDB<K, V> {

    private final HistoricalSegmentTable historicalSegmentTable;
    private final ALoadingCache<K, IReadWriteLock> key_tableLock = new ALoadingCache<K, IReadWriteLock>() {
        @Override
        protected IReadWriteLock loadValue(final K key) {
            final String name = ALiveSegmentedTimeSeriesDB.class.getSimpleName() + "_" + getName() + "_"
                    + hashKeyToString(key) + "_tableLock";
            return newTableLock(name);
        }

        @Override
        protected boolean isHighConcurrency() {
            return true;
        }
    };
    private final ALoadingCache<K, LiveSegmentedTimeSeriesStorageCache<K, V>> key_lookupTableCache;

    public ALiveSegmentedTimeSeriesDB(final String name) {
        this.historicalSegmentTable = new HistoricalSegmentTable(name);
        this.key_lookupTableCache = new ALoadingCache<K, LiveSegmentedTimeSeriesStorageCache<K, V>>() {
            @Override
            protected LiveSegmentedTimeSeriesStorageCache<K, V> loadValue(final K key) {
                return new LiveSegmentedTimeSeriesStorageCache<K, V>(historicalSegmentTable, key,
                        getBatchFlushInterval());
            }

            @Override
            protected boolean isHighConcurrency() {
                return true;
            }
        };
    }

    protected IReadWriteLock newTableLock(final String name) {
        return Locks.newReentrantReadWriteLock(name);
    }

    protected int getBatchFlushInterval() {
        return ATimeSeriesUpdater.BATCH_FLUSH_INTERVAL;
    }

    protected abstract ICloseableIterable<? extends V> downloadSegmentElements(SegmentedKey<K> segmentedKey);

    protected SegmentedTimeSeriesStorage newStorage(final File directory, final Integer valueFixedLength,
            final ICompressionFactory compressionFactory) {
        return new SegmentedTimeSeriesStorage(directory, valueFixedLength, compressionFactory);
    }

    protected void deleteCorruptedStorage(final File directory) {
        directory.delete();
    }

    public abstract ISegmentFinder getSegmentFinder(K key);

    protected SegmentedTimeSeriesStorage getStorage() {
        return historicalSegmentTable.getStorage();
    }

    protected abstract Integer newValueFixedLength();

    protected abstract ISerde<V> newValueSerde();

    protected ICompressionFactory newCompressionFactory() {
        return LZ4Streams.getDefaultCompressionFactory();
    }

    protected abstract FDate extractEndTime(V value);

    public final String hashKeyToString(final K key) {
        return Files.normalizeFilename(innerHashKeyToString(key));
    }

    protected abstract String innerHashKeyToString(K key);

    public final String hashKeyToString(final SegmentedKey<K> segmentedKey) {
        return historicalSegmentTable.hashKeyToString(segmentedKey);
    }

    public abstract FDate getFirstAvailableHistoricalSegmentFrom(K key);

    public abstract FDate getLastAvailableHistoricalSegmentTo(K key, FDate updateTo);

    public final class HistoricalSegmentTable extends ASegmentedTimeSeriesDB<K, V> {
        private HistoricalSegmentTable(final String name) {
            super(name);
        }

        @Override
        public Integer newValueFixedLength() {
            return ALiveSegmentedTimeSeriesDB.this.newValueFixedLength();
        }

        @Override
        public ISerde<V> newValueSerde() {
            return ALiveSegmentedTimeSeriesDB.this.newValueSerde();
        }

        @Override
        protected ICompressionFactory newCompressionFactory() {
            return ALiveSegmentedTimeSeriesDB.this.newCompressionFactory();
        }

        @Override
        public FDate extractEndTime(final V value) {
            return ALiveSegmentedTimeSeriesDB.this.extractEndTime(value);
        }

        @Override
        protected String innerHashKeyToString(final K key) {
            return ALiveSegmentedTimeSeriesDB.this.hashKeyToString(key);
        }

        @Override
        protected SegmentedTimeSeriesStorage newStorage(final File directory, final Integer valueFixedLength,
                final ICompressionFactory compressionFactory) {
            return ALiveSegmentedTimeSeriesDB.this.newStorage(directory, valueFixedLength, compressionFactory);
        }

        @Override
        protected void deleteCorruptedStorage(final File directory) {
            ALiveSegmentedTimeSeriesDB.this.deleteCorruptedStorage(directory);
        }

        @Override
        public SegmentedTimeSeriesStorage getStorage() {
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
        protected String getStorageName(final String name) {
            return ALiveSegmentedTimeSeriesDB.this.getStorageName(name);
        }

        @Override
        protected ICloseableIterable<? extends V> downloadSegmentElements(final SegmentedKey<K> segmentedKey) {
            return ALiveSegmentedTimeSeriesDB.this.downloadSegmentElements(segmentedKey);
        }

        @Override
        public FDate getFirstAvailableHistoricalSegmentFrom(final K key) {
            return ALiveSegmentedTimeSeriesDB.this.getFirstAvailableHistoricalSegmentFrom(key);
        }

        @Override
        public FDate getLastAvailableHistoricalSegmentTo(final K key, final FDate updateTo) {
            return ALiveSegmentedTimeSeriesDB.this.getLastAvailableHistoricalSegmentTo(key, updateTo);
        }

        @Override
        public ASegmentedTimeSeriesStorageCache<K, V> getLookupTableCache(final K key) {
            return super.getLookupTableCache(key);
        }

        @Override
        public ISegmentFinder getSegmentFinder(final K key) {
            return ALiveSegmentedTimeSeriesDB.this.getSegmentFinder(key);
        }

        @Override
        protected String getElementsName() {
            return ALiveSegmentedTimeSeriesDB.this.getElementsName();
        }

        @Override
        protected void onSegmentCompleted(final SegmentedKey<K> segmentedKey,
                final ICloseableIterable<V> segmentValues) {
            ALiveSegmentedTimeSeriesDB.this.onSegmentCompleted(segmentedKey, segmentValues);
        }

        @Override
        protected ITimeSeriesUpdater<SegmentedKey<K>, V> newSegmentUpdaterOverride(final SegmentedKey<K> segmentedKey,
                final ASegmentedTimeSeriesDB<K, V>.SegmentedTable segmentedTable,
                final Function<SegmentedKey<K>, ICloseableIterable<? extends V>> source) {
            return ALiveSegmentedTimeSeriesDB.this.newSegmentUpdaterOverride(segmentedKey, segmentedTable, source);
        }

    }

    protected ITimeSeriesUpdater<SegmentedKey<K>, V> newSegmentUpdaterOverride(final SegmentedKey<K> segmentedKey,
            final ASegmentedTimeSeriesDB<K, V>.SegmentedTable segmentedTable,
            final Function<SegmentedKey<K>, ICloseableIterable<? extends V>> source) {
        return null;
    }

    protected void onSegmentCompleted(final SegmentedKey<K> segmentedKey, final ICloseableIterable<V> segmentValues) {}

    protected abstract String getElementsName();

    @Override
    public synchronized void close() {
        historicalSegmentTable.close();
        for (final LiveSegmentedTimeSeriesStorageCache<K, V> cache : key_lookupTableCache.values()) {
            cache.close();
        }
        key_lookupTableCache.clear();
        key_tableLock.clear();
    }

    @Override
    public File getDirectory() {
        return historicalSegmentTable.getDirectory();
    }

    @Override
    public IReadWriteLock getTableLock(final K key) {
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
        final ILock writeLock = getTableLock(key).writeLock();
        try {
            if (!writeLock.tryLock(1, TimeUnit.MINUTES)) {
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
            getLookupTableCache(key).deleteAll();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public String getName() {
        return historicalSegmentTable.getName();
    }

    public void maybeInitSegment(final SegmentedKey<K> segmentedKey) {
        historicalSegmentTable.getLookupTableCache(segmentedKey.getKey()).maybeInitSegment(segmentedKey);
    }

    private LiveSegmentedTimeSeriesStorageCache<K, V> getLookupTableCache(final K key) {
        return key_lookupTableCache.get(key);
    }

    @Override
    public ICloseableIterable<V> rangeValues(final K key, final FDate from, final FDate to) {
        return new RangeValues(key, from, to);
    }

    @Override
    public ICloseableIterable<V> rangeReverseValues(final K key, final FDate from, final FDate to) {
        return new RangeReverseValues(from, to, key);
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
            return extractEndTime(value);
        }
    }

    @Override
    public V getPreviousValue(final K key, final FDate date, final int shiftBackUnits) {
        final Lock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            if (date == null) {
                return null;
            } else if (date.isBeforeOrEqualTo(FDate.MIN_DATE)) {
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
            return extractEndTime(value);
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

    protected String getStorageName(final String name) {
        return ATimeSeriesDB.getDefaultStorageName(name);
    }

    public void putNextLiveValue(final K key, final V nextLiveValue) {
        final Lock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            getLookupTableCache(key).putNextLiveValue(nextLiveValue);
        } finally {
            readLock.unlock();
        }
    }

    private final class RangeReverseValues implements ICloseableIterable<V> {
        private final FDate from;
        private final FDate to;
        private final K key;

        private RangeReverseValues(final FDate from, final FDate to, final K key) {
            this.from = from;
            this.to = to;
            this.key = key;
        }

        @Override
        public ICloseableIterator<V> iterator() {
            return new ACloseableIterator<V>(
                    new TextDescription("%s: %s(%s, %s, %s)", ALiveSegmentedTimeSeriesDB.class.getSimpleName(),
                            RangeReverseValues.class.getSimpleName(), key, from, to)) {

                private final RangeValuesFinalizer<V> finalizer = new RangeValuesFinalizer<>();

                {
                    this.finalizer.register(this);
                }

                private ICloseableIterator<V> getReadRangeValues() {
                    if (finalizer.readRangeValues == null) {
                        finalizer.readRangeValues = getLookupTableCache(key)
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
            return new ACloseableIterator<V>(
                    new TextDescription("%s: %s(%s, %s, %s)", ALiveSegmentedTimeSeriesDB.class.getSimpleName(),
                            RangeValues.class.getSimpleName(), key, from, to)) {

                private final RangeValuesFinalizer<V> finalizer = new RangeValuesFinalizer<>();

                {
                    this.finalizer.register(this);
                }

                private ICloseableIterator<V> getReadRangeValues() {
                    if (finalizer.readRangeValues == null) {
                        finalizer.readRangeValues = getLookupTableCache(key)
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
}
