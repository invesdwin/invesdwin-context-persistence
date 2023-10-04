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
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupMode;
import de.invesdwin.context.persistence.timeseriesdb.segmented.ASegmentedTimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.segmented.ISegmentedTimeSeriesDBInternals;
import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentedTimeSeriesStorage;
import de.invesdwin.context.persistence.timeseriesdb.segmented.finder.ISegmentFinder;
import de.invesdwin.context.persistence.timeseriesdb.segmented.live.segment.ILiveSegment;
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
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;

@ThreadSafe
public abstract class ALiveSegmentedTimeSeriesDB<K, V> implements ILiveSegmentedTimeSeriesDBInternals<K, V> {

    private final ISerde<V> valueSerde;
    private final Integer valueFixedLength;
    private final ICompressionFactory compressionFactory;
    private final TimeSeriesLookupMode lookupMode;
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
    private final ALoadingCache<K, ALiveSegmentedTimeSeriesStorageCache<K, V>> key_liveSegmentedLookupTableCache;

    public ALiveSegmentedTimeSeriesDB(final String name) {
        this.valueSerde = newValueSerde();
        this.valueFixedLength = newValueFixedLength();
        this.compressionFactory = newCompressionFactory();
        this.lookupMode = newLookupMode();
        this.historicalSegmentTable = new HistoricalSegmentTable(name);
        this.key_liveSegmentedLookupTableCache = new ALoadingCache<K, ALiveSegmentedTimeSeriesStorageCache<K, V>>() {
            @Override
            protected ALiveSegmentedTimeSeriesStorageCache<K, V> loadValue(final K key) {
                return new ALiveSegmentedTimeSeriesStorageCache<K, V>(historicalSegmentTable, key,
                        getBatchFlushInterval()) {

                    @Override
                    protected ILiveSegment<K, V> newLiveSegment(final SegmentedKey<K> segmentedKey,
                            final ISegmentedTimeSeriesDBInternals<K, V> historicalSegmentTable,
                            final int batchFlushInterval) {
                        return ALiveSegmentedTimeSeriesDB.this.newLiveSegment(segmentedKey, historicalSegmentTable,
                                batchFlushInterval);
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
        return valueSerde;
    }

    @Override
    public Integer getValueFixedLength() {
        return valueFixedLength;
    }

    @Override
    public ICompressionFactory getCompressionFactory() {
        return compressionFactory;
    }

    @Override
    public TimeSeriesLookupMode getLookupMode() {
        return lookupMode;
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
            return ALiveSegmentedTimeSeriesDB.this.getValueFixedLength();
        }

        @Override
        public ISerde<V> newValueSerde() {
            return ALiveSegmentedTimeSeriesDB.this.getValueSerde();
        }

        @Override
        protected ICompressionFactory newCompressionFactory() {
            return ALiveSegmentedTimeSeriesDB.this.getCompressionFactory();
        }

        @Override
        public TimeSeriesLookupMode getLookupMode() {
            return ALiveSegmentedTimeSeriesDB.this.getLookupMode();
        }

        @Override
        public FDate extractStartTime(final V value) {
            return ALiveSegmentedTimeSeriesDB.this.extractStartTime(value);
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
        public File getBaseDirectory() {
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
            super.onSegmentCompleted(segmentedKey, segmentValues);
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

    protected void onFlushLiveSegmentCompleted(final ILiveSegment<K, V> liveSegment,
            final ICloseableIterable<V> flushedValues) {}

    protected ILiveSegment<K, V> newLiveSegment(final SegmentedKey<K> segmentedKey,
            final ISegmentedTimeSeriesDBInternals<K, V> historicalSegmentTable, final int batchFlushInterval) {
        return ALiveSegmentedTimeSeriesStorageCache.newDefaultLiveSegment(segmentedKey, historicalSegmentTable,
                batchFlushInterval);
    }

    protected abstract String getElementsName();

    @Override
    public synchronized void close() {
        historicalSegmentTable.close();
        for (final ALiveSegmentedTimeSeriesStorageCache<K, V> cache : key_liveSegmentedLookupTableCache.values()) {
            cache.close();
        }
        key_liveSegmentedLookupTableCache.clear();
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
            return getLiveSegmentedLookupTableCache(key).isEmptyOrInconsistent();
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
            getLiveSegmentedLookupTableCache(key).deleteAll();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public String getName() {
        return historicalSegmentTable.getName();
    }

    public void maybeInitSegment(final SegmentedKey<K> segmentedKey) {
        historicalSegmentTable.getSegmentedLookupTableCache(segmentedKey.getKey()).maybeInitSegment(segmentedKey);
    }

    @Override
    public ALiveSegmentedTimeSeriesStorageCache<K, V> getLiveSegmentedLookupTableCache(final K key) {
        return key_liveSegmentedLookupTableCache.get(key);
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
    public V getLatestValue(final K key, final FDate date) {
        final Lock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            if (date.isBeforeOrEqualTo(FDates.MIN_DATE)) {
                return getLiveSegmentedLookupTableCache(key).getFirstValue();
            } else if (date.isAfterOrEqualTo(FDates.MAX_DATE)) {
                return getLiveSegmentedLookupTableCache(key).getLastValue();
            } else {
                return getLiveSegmentedLookupTableCache(key).getLatestValue(date);
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
        final Lock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            final ALiveSegmentedTimeSeriesStorageCache<K, V> lookupTableCache = getLiveSegmentedLookupTableCache(key);
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
        final Lock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            if (date.isBeforeOrEqualTo(FDates.MIN_DATE)) {
                if (getLiveSegmentedLookupTableCache(key).size() == 0) {
                    return -1L;
                } else {
                    return 0L;
                }
            } else if (date.isAfterOrEqualTo(FDates.MAX_DATE)) {
                return getLiveSegmentedLookupTableCache(key).size() - 1;
            } else {
                return getLiveSegmentedLookupTableCache(key).getLatestValueIndex(date);
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long size(final K key) {
        final Lock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            return getLiveSegmentedLookupTableCache(key).size();
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
            } else if (date.isBeforeOrEqualTo(FDates.MIN_DATE)) {
                return getLiveSegmentedLookupTableCache(key).getFirstValue();
            } else {
                return getLiveSegmentedLookupTableCache(key).getPreviousValue(date, shiftBackUnits);
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
            if (date.isAfterOrEqualTo(FDates.MAX_DATE)) {
                return getLiveSegmentedLookupTableCache(key).getLastValue();
            } else {
                return getLiveSegmentedLookupTableCache(key).getNextValue(date, shiftForwardUnits);
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

    public void putNextLiveValue(final K key, final V nextLiveValue) {
        final Lock readLock = getTableLock(key).readLock();
        readLock.lock();
        try {
            getLiveSegmentedLookupTableCache(key).putNextLiveValue(nextLiveValue);
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
                        finalizer.readRangeValues = getLiveSegmentedLookupTableCache(key)
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
                        finalizer.readRangeValues = getLiveSegmentedLookupTableCache(key)
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
