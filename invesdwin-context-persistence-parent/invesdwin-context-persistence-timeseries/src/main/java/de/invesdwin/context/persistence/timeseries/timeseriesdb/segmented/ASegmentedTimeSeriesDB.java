package de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented;

import java.io.File;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.ITimeSeriesDB;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.storage.TimeSeriesStorage;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.updater.ITimeSeriesUpdater;
import de.invesdwin.util.collections.iterable.ACloseableIterator;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import de.invesdwin.util.collections.loadingcache.historical.AHistoricalCache;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.Locks;
import de.invesdwin.util.concurrent.lock.readwrite.IReadWriteLock;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.description.TextDescription;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.range.TimeRange;
import ezdb.serde.Serde;
import net.jpountz.lz4.LZ4BlockOutputStream;

@ThreadSafe
public abstract class ASegmentedTimeSeriesDB<K, V> implements ITimeSeriesDB<K, V> {

    private final SegmentedTable segmentedTable;
    private final ALoadingCache<K, IReadWriteLock> key_tableLock = new ALoadingCache<K, IReadWriteLock>() {
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
    private final ALoadingCache<K, ASegmentedTimeSeriesStorageCache<K, V>> key_lookupTableCache;

    public ASegmentedTimeSeriesDB(final String name) {
        this.segmentedTable = new SegmentedTable(name);
        this.key_lookupTableCache = new ALoadingCache<K, ASegmentedTimeSeriesStorageCache<K, V>>() {
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
                    protected AHistoricalCache<TimeRange> getSegmentFinder(final K key) {
                        return ASegmentedTimeSeriesDB.this.getSegmentFinder(key);
                    }

                    @Override
                    protected LZ4BlockOutputStream newCompressor(final OutputStream out) {
                        return ASegmentedTimeSeriesDB.this.newCompressor(out);
                    }

                    @Override
                    protected String getElementsName() {
                        return ASegmentedTimeSeriesDB.this.getElementsName();
                    }

                    @Override
                    public void onSegmentCompleted(final SegmentedKey<K> segmentedKey,
                            final ICloseableIterable<V> segmentValues) {
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

    protected ITimeSeriesUpdater<SegmentedKey<K>, V> newSegmentUpdaterOverride(final SegmentedKey<K> segmentedKey,
            final ASegmentedTimeSeriesDB<K, V>.SegmentedTable segmentedTable,
            final Function<SegmentedKey<K>, ICloseableIterable<? extends V>> source) {
        return null;
    }

    protected void onSegmentCompleted(final SegmentedKey<K> segmentedKey, final ICloseableIterable<V> segmentValues) {
    }

    protected abstract String getElementsName();

    protected LZ4BlockOutputStream newCompressor(final OutputStream out) {
        return ATimeSeriesUpdater.newDefaultCompressor(out);
    }

    protected abstract ICloseableIterable<? extends V> downloadSegmentElements(SegmentedKey<K> segmentedKey);

    protected SegmentedTimeSeriesStorage newStorage(final File directory, final Integer valueFixedLength) {
        return new SegmentedTimeSeriesStorage(directory, valueFixedLength);
    }

    protected SegmentedTable getSegmentedTable() {
        return segmentedTable;
    }

    protected void deleteCorruptedStorage(final File directory) {
        directory.delete();
    }

    public abstract AHistoricalCache<TimeRange> getSegmentFinder(K key);

    protected SegmentedTimeSeriesStorage getStorage() {
        return (SegmentedTimeSeriesStorage) segmentedTable.getStorage();
    }

    protected abstract Integer newFixedLength();

    protected abstract Serde<V> newValueSerde();

    protected abstract FDate extractEndTime(V value);

    public final String hashKeyToString(final K key) {
        return Files.normalizeFilename(innerHashKeyToString(key));
    }

    protected abstract String innerHashKeyToString(K key);

    public final String hashKeyToString(final SegmentedKey<K> key) {
        return ASegmentedTimeSeriesDB.this.hashKeyToString(key.getKey()) + "-"
                + key.getSegment().getFrom().toString(FDate.FORMAT_UNDERSCORE_DATE_TIME_MS) + "-"
                + key.getSegment().getTo().toString(FDate.FORMAT_UNDERSCORE_DATE_TIME_MS);
    }

    public abstract FDate getFirstAvailableHistoricalSegmentFrom(K key);

    public abstract FDate getLastAvailableHistoricalSegmentTo(K key, FDate updateTo);

    @Override
    public synchronized void close() {
        segmentedTable.close();
        for (final ASegmentedTimeSeriesStorageCache<?, ?> cache : key_lookupTableCache.values()) {
            cache.close();
        }
        key_lookupTableCache.clear();
        key_tableLock.clear();
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
        return segmentedTable.getName();
    }

    protected ASegmentedTimeSeriesStorageCache<K, V> getLookupTableCache(final K key) {
        return key_lookupTableCache.get(key);
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
            return new ACloseableIterator<V>(new TextDescription("%s: %s(%s, %s, %s)",
                    ASegmentedTimeSeriesDB.class.getSimpleName(), RangeValues.class.getSimpleName(), key, from, to)) {

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

    public final class SegmentedTable extends ATimeSeriesDB<SegmentedKey<K>, V> {
        private SegmentedTable(final String name) {
            super(name);
        }

        @Override
        public Serde<V> getValueSerde() {
            return super.getValueSerde();
        }

        @Override
        protected Integer newValueFixedLength() {
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
        public FDate extractEndTime(final V value) {
            return ASegmentedTimeSeriesDB.this.extractEndTime(value);
        }

        @Override
        protected String innerHashKeyToString(final SegmentedKey<K> key) {
            return ASegmentedTimeSeriesDB.this.hashKeyToString(key);
        }

        @Override
        protected TimeSeriesStorage newStorage(final File directory, final Integer valueFixedLength) {
            return ASegmentedTimeSeriesDB.this.newStorage(directory, valueFixedLength);
        }

        @Override
        protected void deleteCorruptedStorage(final File directory) {
            ASegmentedTimeSeriesDB.this.deleteCorruptedStorage(directory);
        }

        @Override
        public TimeSeriesStorage getStorage() {
            return super.getStorage();
        }

        @Override
        protected File getBaseDirectory() {
            return ASegmentedTimeSeriesDB.this.getBaseDirectory();
        }

        public LZ4BlockOutputStream newCompressor(final OutputStream out) {
            return ASegmentedTimeSeriesDB.this.newCompressor(out);
        }

    }

}
