package de.invesdwin.context.persistence.timeseriesdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SerializationException;

import de.invesdwin.context.integration.compression.DisabledCompressionFactory;
import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.timeseriesdb.buffer.ArrayFileBufferCacheResult;
import de.invesdwin.context.persistence.timeseriesdb.buffer.FileBufferCache;
import de.invesdwin.context.persistence.timeseriesdb.buffer.IFileBufferCacheResult;
import de.invesdwin.context.persistence.timeseriesdb.buffer.source.ByteBufferFileBufferSource;
import de.invesdwin.context.persistence.timeseriesdb.buffer.source.IFileBufferSource;
import de.invesdwin.context.persistence.timeseriesdb.buffer.source.IterableFileBufferSource;
import de.invesdwin.context.persistence.timeseriesdb.loop.AShiftBackUnitsLoopLongIndex;
import de.invesdwin.context.persistence.timeseriesdb.loop.AShiftForwardUnitsLoopLongIndex;
import de.invesdwin.context.persistence.timeseriesdb.loop.ShiftBackUnitsLoop;
import de.invesdwin.context.persistence.timeseriesdb.loop.ShiftForwardUnitsLoop;
import de.invesdwin.context.persistence.timeseriesdb.storage.ISkipFileFunction;
import de.invesdwin.context.persistence.timeseriesdb.storage.MemoryFileMetadata;
import de.invesdwin.context.persistence.timeseriesdb.storage.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.MemoryFileSummaryByteBuffer;
import de.invesdwin.context.persistence.timeseriesdb.storage.SingleValue;
import de.invesdwin.context.persistence.timeseriesdb.storage.TimeSeriesStorage;
import de.invesdwin.context.persistence.timeseriesdb.storage.cache.ALatestValueByIndexCache;
import de.invesdwin.context.persistence.timeseriesdb.storage.key.RangeShiftUnitsKey;
import de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdaterInternalMethods;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.collections.eviction.EvictionMode;
import de.invesdwin.util.collections.iterable.ACloseableIterator;
import de.invesdwin.util.collections.iterable.ATransformingIterator;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterator;
import de.invesdwin.util.collections.iterable.FlatteningIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.PeekingCloseableIterator;
import de.invesdwin.util.collections.iterable.skip.ASkippingIterator;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.collections.loadingcache.ILoadingCache;
import de.invesdwin.util.collections.loadingcache.caffeine.ACaffeineLoadingCache;
import de.invesdwin.util.collections.loadingcache.historical.AHistoricalCache;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.disabled.DisabledLock;
import de.invesdwin.util.concurrent.reference.MutableSoftReference;
import de.invesdwin.util.concurrent.reference.WeakThreadLocalReference;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.OperatingSystem;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.marshallers.serde.FromBufferDelegateSerde;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.streams.PreLockedDelegateInputStream;
import de.invesdwin.util.streams.buffer.file.IMemoryMappedFile;
import de.invesdwin.util.streams.pool.PooledFastByteArrayOutputStream;
import de.invesdwin.util.streams.pool.buffered.BufferedFileDataInputStream;
import de.invesdwin.util.streams.pool.buffered.PreLockedBufferedFileDataInputStream;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.duration.Duration;
import ezdb.table.RangeTableRow;

@NotThreadSafe
public class TimeSeriesStorageCache<K, V> {
    public static final Integer MAXIMUM_SIZE = TimeSeriesProperties.STORAGE_CACHE_MAXIMUM_SIZE;
    public static final EvictionMode EVICTION_MODE = AHistoricalCache.EVICTION_MODE;
    public static final Duration EXPIRE_AFTER_ACCESS = TimeSeriesProperties.STORAGE_CACHE_EVICTION_TIMEOUT;

    private static final String READ_RANGE_VALUES = "readRangeValues";
    private static final String READ_RANGE_VALUES_REVERSE = "readRangeValuesReverse";
    private static final Function<RangeTableRow<String, FDate, MemoryFileSummary>, FDate> EXTRACT_END_TIME_FROM_RANGE_KEYS = (
            r) -> r.getRangeKey();
    private final TimeSeriesStorage storage;
    private final ILoadingCache<FDate, RangeTableRow<String, FDate, MemoryFileSummary>> fileLookupTable_latestRangeKeyCache = new ACaffeineLoadingCache<FDate, RangeTableRow<String, FDate, MemoryFileSummary>>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return MAXIMUM_SIZE;
        }

        @Override
        protected EvictionMode getEvictionMode() {
            return EVICTION_MODE;
        }

        @Override
        protected boolean isHighConcurrency() {
            return true;
        }

        @Override
        protected Duration getExpireAfterAccess() {
            return EXPIRE_AFTER_ACCESS;
        }

        @Override
        protected RangeTableRow<String, FDate, MemoryFileSummary> loadValue(final FDate key) {
            return newLatestRangeKey(key);
        }
    };
    private final ILoadingCache<Long, RangeTableRow<String, FDate, MemoryFileSummary>> fileLookupTable_latestRangeKeyIndexCache = new ACaffeineLoadingCache<Long, RangeTableRow<String, FDate, MemoryFileSummary>>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return MAXIMUM_SIZE;
        }

        @Override
        protected EvictionMode getEvictionMode() {
            return EVICTION_MODE;
        }

        @Override
        protected boolean isHighConcurrency() {
            return true;
        }

        @Override
        protected Duration getExpireAfterAccess() {
            return EXPIRE_AFTER_ACCESS;
        }

        @Override
        protected RangeTableRow<String, FDate, MemoryFileSummary> loadValue(final Long key) {
            return newLatestRangeKeyIndex(key);
        }
    };
    private final ILoadingCache<FDate, Long> latestValueIndexLookupCache = new ACaffeineLoadingCache<FDate, Long>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return MAXIMUM_SIZE;
        };

        @Override
        protected EvictionMode getEvictionMode() {
            return EVICTION_MODE;
        }

        @Override
        protected boolean isHighConcurrency() {
            return true;
        }

        @Override
        protected Duration getExpireAfterAccess() {
            return EXPIRE_AFTER_ACCESS;
        }

        @Override
        protected Long loadValue(final FDate key) {
            return latestValueIndexLookup(key);
        }
    };
    private final ILoadingCache<RangeShiftUnitsKey, Long> previousValueIndexLookupCache = new ACaffeineLoadingCache<RangeShiftUnitsKey, Long>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return MAXIMUM_SIZE;
        }

        @Override
        protected EvictionMode getEvictionMode() {
            return EVICTION_MODE;
        }

        @Override
        protected boolean isHighConcurrency() {
            return true;
        }

        @Override
        protected Duration getExpireAfterAccess() {
            return EXPIRE_AFTER_ACCESS;
        }

        @Override
        protected Long loadValue(final RangeShiftUnitsKey key) {
            return previousValueIndexLookup(key.getRangeKey(), key.getShiftUnits());
        }
    };
    private final ILoadingCache<RangeShiftUnitsKey, Long> nextValueIndexLookupCache = new ACaffeineLoadingCache<RangeShiftUnitsKey, Long>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return MAXIMUM_SIZE;
        }

        @Override
        protected EvictionMode getEvictionMode() {
            return EVICTION_MODE;
        }

        @Override
        protected boolean isHighConcurrency() {
            return true;
        }

        @Override
        protected Duration getExpireAfterAccess() {
            return EXPIRE_AFTER_ACCESS;
        }

        @Override
        protected Long loadValue(final RangeShiftUnitsKey key) {
            return nextValueIndexLookup(key.getRangeKey(), key.getShiftUnits());
        }
    };
    private final WeakThreadLocalReference<ALatestValueByIndexCache<V>> latestValueByIndexCacheHolder = new WeakThreadLocalReference<ALatestValueByIndexCache<V>>() {
        @Override
        protected ALatestValueByIndexCache<V> initialValue() {
            return new LatestValueByIndexCache();
        };
    };
    private volatile int lastResetIndex = 0;

    private final String hashKey;
    private final ISerde<V> valueSerde;
    private final Integer fixedLength;
    private final Function<V, FDate> extractEndTime;
    private final boolean flyweight;
    private final TimeSeriesLookupMode lookupMode;
    @GuardedBy("this")
    private File dataDirectory;

    private volatile Optional<V> cachedFirstValue;
    private volatile Optional<V> cachedLastValue;
    private volatile long cachedSize = -1L;
    /**
     * keeping the range keys outside of the concurrent linked hashmap of the ADelegateRangeTable with memory write
     * through to disk is still better for increased parallelity and for not having to iterate through each element of
     * the other hashkeys.
     */
    private volatile MutableSoftReference<ArrayFileBufferCacheResult<RangeTableRow<String, FDate, MemoryFileSummary>>> cachedAllRangeKeys = new MutableSoftReference<ArrayFileBufferCacheResult<RangeTableRow<String, FDate, MemoryFileSummary>>>(
            null);
    private final Log log = new Log(this);
    @GuardedBy("this")
    private MemoryFileMetadata memoryFileMetadata;

    public TimeSeriesStorageCache(final TimeSeriesStorage storage, final String hashKey, final ISerde<V> valueSerde,
            final Integer fixedLength, final Function<V, FDate> extractTime, final TimeSeriesLookupMode lookupMode) {
        this.storage = storage;
        this.hashKey = hashKey;
        this.valueSerde = valueSerde;
        this.fixedLength = fixedLength;
        this.extractEndTime = extractTime;
        final boolean compressed = storage.getCompressionFactory() != DisabledCompressionFactory.INSTANCE;
        final boolean mmap = TimeSeriesProperties.FILE_BUFFER_CACHE_MMAP_ENABLED;
        this.flyweight = !compressed && mmap && fixedLength != null && fixedLength > 0;
        this.lookupMode = lookupMode;
    }

    public synchronized File getDataDirectory() {
        if (dataDirectory == null) {
            dataDirectory = newDataDirectory();
            try {
                Files.forceMkdir(dataDirectory);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
        return dataDirectory;
    }

    public File newDataDirectory() {
        return storage.newDataDirectory(hashKey);
    }

    public File getUpdateLockFile() {
        return new File(getDataDirectory(), "updateRunning.lock");
    }

    public static File newMemoryFile(final ITimeSeriesUpdaterInternalMethods<?, ?> parent,
            final long precedingMemoryOffset) {
        final File memoryFile = new File(parent.getLookupTable().getDataDirectory(), "memory.data");
        return newMemoryFile(memoryFile, precedingMemoryOffset);
    }

    public static File newMemoryFile(final File memoryFile, final long precedingMemoryOffset) {
        if (OperatingSystem.isWindows()) {
            return new File(memoryFile.getAbsolutePath() + "." + precedingMemoryOffset);
        } else {
            return memoryFile;
        }
    }

    public synchronized MemoryFileMetadata getMemoryFileMetadata() {
        if (memoryFileMetadata == null) {
            memoryFileMetadata = new MemoryFileMetadata(getDataDirectory());
        }
        return memoryFileMetadata;
    }

    public void finishFile(final FDate time, final V firstValue, final V lastValue, final long precedingValueCount,
            final int valueCount, final File memoryFile, final long precedingMemoryOffset, final long memoryOffset,
            final long memoryLength) {
        final MemoryFileSummary summary = new MemoryFileSummary(valueSerde, firstValue, lastValue, precedingValueCount,
                valueCount, memoryFile.getAbsolutePath(), precedingMemoryOffset, memoryOffset, memoryLength);
        assertSummary(summary);
        storage.getFileLookupTable().put(hashKey, time, summary);
        final long memoryFileSize = precedingMemoryOffset + memoryFile.length();
        final long expectedMemoryFileSize = precedingMemoryOffset + memoryOffset + memoryLength;
        if (memoryFileSize != expectedMemoryFileSize) {
            throw new IllegalStateException(
                    "memoryFileSize[" + memoryFileSize + "] != expectedMemoryFileSize[" + expectedMemoryFileSize + "]");
        }
        final MemoryFileMetadata metadata = getMemoryFileMetadata();
        final long prevMemoryFileSize = metadata.getExpectedMemoryFileSize();
        if (prevMemoryFileSize > expectedMemoryFileSize) {
            throw new IllegalStateException("memoryFileFize[" + memoryFileSize
                    + "] should not be less than prevMemoryFileSize[" + prevMemoryFileSize + "]");
        }
        metadata.setExpectedMemoryFileSize(expectedMemoryFileSize);
        final FDate firstValueDate = extractEndTime.apply(firstValue);
        final FDate lastValueDate = extractEndTime.apply(lastValue);
        metadata.setSummary(time, firstValueDate, lastValueDate, precedingValueCount, valueCount,
                memoryFile.getAbsolutePath(), precedingMemoryOffset, memoryOffset, memoryLength);
        clearCaches();
    }

    public void finishFile(final FDate time, final MemoryFileSummary summary) {
        assertSummary(summary);
        storage.getFileLookupTable().put(hashKey, time, summary);
        clearCaches();
    }

    private void assertSummary(final MemoryFileSummary summary) {
        final MemoryFileSummary prevSummary = storage.getFileLookupTable().getLatestValue(hashKey, FDates.MAX_DATE);
        assertSummary(prevSummary, summary);
    }

    private void assertSummary(final MemoryFileSummary prevSummary, final MemoryFileSummary summary) {
        final V firstValue = summary.getFirstValue(valueSerde);
        final FDate firstValueTime = extractEndTime.apply(firstValue);
        if (prevSummary != null) {
            final V precedingLastValue = prevSummary.getLastValue(valueSerde);
            final FDate precedingLastValueTime = extractEndTime.apply(precedingLastValue);
            if (precedingLastValueTime.isAfter(firstValueTime)) {
                throw new IllegalStateException("precedingLastValueTime [" + precedingLastValueTime
                        + "] should not be after firstValueTime [" + firstValueTime + "]");
            }
        }
        final V lastValue = summary.getLastValue(valueSerde);
        final FDate lastValueTime = extractEndTime.apply(lastValue);
        if (firstValueTime.isAfterNotNullSafe(lastValueTime)) {
            throw new IllegalStateException("firstValueTime [" + firstValueTime
                    + "] should not be after lastValueTime [" + lastValueTime + "]");
        }
    }

    protected ICloseableIterable<MemoryFileSummary> readRangeFiles(final FDate from, final FDate to,
            final ILock readLock, final ISkipFileFunction skipFileFunction) {
        return new ICloseableIterable<MemoryFileSummary>() {

            @Override
            public ICloseableIterator<MemoryFileSummary> iterator() {
                final FDate usedFrom;
                if (from == null) {
                    final V firstValue = getFirstValue();
                    if (firstValue == null) {
                        return EmptyCloseableIterator.getInstance();
                    }
                    usedFrom = extractEndTime.apply(firstValue);
                } else {
                    usedFrom = from;
                }
                return new ACloseableIterator<MemoryFileSummary>(new TextDescription("%s[%s]: readRangeFiles(%s, %s)",
                        TimeSeriesStorageCache.class.getSimpleName(), hashKey, from, to)) {

                    //use latest time available even if delegate iterator has no values
                    private RangeTableRow<String, FDate, MemoryFileSummary> latestFirstTime = getLatestRangeKey(
                            usedFrom);
                    private final ICloseableIterator<RangeTableRow<String, FDate, MemoryFileSummary>> delegate;

                    {
                        if (latestFirstTime == null) {
                            delegate = EmptyCloseableIterator.getInstance();
                        } else {
                            delegate = getRangeKeys(latestFirstTime.getRangeKey().addMilliseconds(1), to);
                        }
                    }

                    @Override
                    protected boolean innerHasNext() {
                        return latestFirstTime != null || delegate.hasNext();
                    }

                    private ICloseableIterator<RangeTableRow<String, FDate, MemoryFileSummary>> getRangeKeys(
                            final FDate from, final FDate to) {
                        final ArrayFileBufferCacheResult<RangeTableRow<String, FDate, MemoryFileSummary>> rangeSource = getAllRangeKeys(
                                readLock);
                        final ICloseableIterator<RangeTableRow<String, FDate, MemoryFileSummary>> rangeFiltered = rangeSource
                                .iterator(EXTRACT_END_TIME_FROM_RANGE_KEYS, from, to);
                        if (skipFileFunction != null) {
                            return new ASkippingIterator<RangeTableRow<String, FDate, MemoryFileSummary>>(
                                    rangeFiltered) {
                                @Override
                                protected boolean skip(final RangeTableRow<String, FDate, MemoryFileSummary> element) {
                                    if (!rangeFiltered.hasNext()) {
                                        /*
                                         * cannot optimize this further for multiple segments because we don't know if a
                                         * segment further back might be empty or not and thus the last segment of
                                         * interest might have been the previous one from which we skipped the last file
                                         * falsely
                                         */
                                        return false;
                                    }
                                    return skipFileFunction.skipFile(element.getValue());
                                }
                            };
                        } else {
                            return rangeFiltered;
                        }
                    }

                    @Override
                    protected MemoryFileSummary innerNext() {
                        final MemoryFileSummary summary;
                        if (latestFirstTime != null) {
                            summary = latestFirstTime.getValue();
                            latestFirstTime = null;
                        } else {
                            summary = delegate.next().getValue();
                        }
                        return summary;
                    }

                    @Override
                    protected void innerClose() {
                        delegate.close();
                    }

                };
            }
        };
    }

    private RangeTableRow<String, FDate, MemoryFileSummary> getLatestRangeKey(final FDate key) {
        return fileLookupTable_latestRangeKeyCache.get(key);
    }

    private RangeTableRow<String, FDate, MemoryFileSummary> newLatestRangeKey(final FDate key) {
        return storage.getFileLookupTable().getLatest(hashKey, key);
    }

    private RangeTableRow<String, FDate, MemoryFileSummary> getLatestRangeKeyIndex(final long key) {
        return fileLookupTable_latestRangeKeyIndexCache.get(key);
    }

    private RangeTableRow<String, FDate, MemoryFileSummary> newLatestRangeKeyIndex(final long key) {
        final ArrayList<RangeTableRow<String, FDate, MemoryFileSummary>> rows = getAllRangeKeys(DisabledLock.INSTANCE)
                .getList();
        if (rows.isEmpty()) {
            return null;
        }
        if (key <= 0) {
            return rows.get(0);
        }
        for (int i = 0; i < rows.size(); i++) {
            final RangeTableRow<String, FDate, MemoryFileSummary> row = rows.get(i);
            final MemoryFileSummary summary = row.getValue();
            if (summary.getPrecedingValueCount() <= key && key < summary.getCombinedValueCount()) {
                return row;
            }
        }
        return rows.get(rows.size() - 1);
    }

    protected ICloseableIterable<MemoryFileSummary> readRangeFilesReverse(final FDate from, final FDate to,
            final ILock readLock, final ISkipFileFunction skipFileFunction) {
        return new ICloseableIterable<MemoryFileSummary>() {

            @Override
            public ICloseableIterator<MemoryFileSummary> iterator() {
                final FDate usedFrom;
                if (from == null) {
                    final V lastValue = getLastValue();
                    if (lastValue == null) {
                        return EmptyCloseableIterator.getInstance();
                    }
                    usedFrom = extractEndTime.apply(lastValue);
                } else {
                    usedFrom = from;
                }
                return new ACloseableIterator<MemoryFileSummary>(
                        new TextDescription("%s[%s]: readRangeFilesReverse(%s, %s)",
                                TimeSeriesStorageCache.class.getSimpleName(), hashKey, from, to)) {

                    //use latest time available even if delegate iterator has no values
                    private RangeTableRow<String, FDate, MemoryFileSummary> latestLastTime = getLatestRangeKey(
                            usedFrom);
                    // add 1 ms to not collide with firstTime
                    private final ICloseableIterator<RangeTableRow<String, FDate, MemoryFileSummary>> delegate;

                    {
                        if (latestLastTime == null) {
                            delegate = EmptyCloseableIterator.getInstance();
                        } else {
                            delegate = getRangeKeysReverse(latestLastTime.getRangeKey().addMilliseconds(-1), to);
                        }
                    }

                    @Override
                    protected boolean innerHasNext() {
                        return latestLastTime != null || delegate.hasNext();
                    }

                    private ICloseableIterator<RangeTableRow<String, FDate, MemoryFileSummary>> getRangeKeysReverse(
                            final FDate from, final FDate to) {
                        final ArrayFileBufferCacheResult<RangeTableRow<String, FDate, MemoryFileSummary>> rangeSource = getAllRangeKeys(
                                readLock);
                        final ICloseableIterator<RangeTableRow<String, FDate, MemoryFileSummary>> rangeFiltered = rangeSource
                                .reverseIterator(EXTRACT_END_TIME_FROM_RANGE_KEYS, from, to);
                        if (skipFileFunction != null) {
                            return new ASkippingIterator<RangeTableRow<String, FDate, MemoryFileSummary>>(
                                    rangeFiltered) {

                                @Override
                                protected boolean skip(final RangeTableRow<String, FDate, MemoryFileSummary> element) {
                                    if (!rangeFiltered.hasNext()) {
                                        /*
                                         * cannot optimize this further for multiple segments because we don't know if a
                                         * segment further back might be empty or not and thus the last segment of
                                         * interest might have been the previous one from which we skipped the last file
                                         * falsely
                                         */
                                        return false;
                                    }
                                    return skipFileFunction.skipFile(element.getValue());
                                }
                            };
                        } else {
                            return rangeFiltered;
                        }
                    }

                    @Override
                    protected MemoryFileSummary innerNext() {
                        final MemoryFileSummary summary;
                        if (latestLastTime != null) {
                            summary = latestLastTime.getValue();
                            latestLastTime = null;
                        } else {
                            summary = delegate.next().getValue();
                        }
                        return summary;
                    }

                    @Override
                    protected void innerClose() {
                        delegate.close();
                    }

                };
            }
        };
    }

    public ICloseableIterator<V> readRangeValues(final FDate from, final FDate to, final ILock readLock,
            final ISkipFileFunction skipFileFunction) {
        final PeekingCloseableIterator<MemoryFileSummary> fileIterator = new PeekingCloseableIterator<MemoryFileSummary>(
                readRangeFiles(from, to, readLock, skipFileFunction).iterator());
        final ICloseableIterator<ICloseableIterator<V>> chunkIterator = new ATransformingIterator<MemoryFileSummary, ICloseableIterator<V>>(
                fileIterator) {
            @Override
            protected ICloseableIterator<V> transform(final MemoryFileSummary value) {
                if (TimeSeriesProperties.FILE_BUFFER_CACHE_PRELOAD_ENABLED) {
                    try {
                        preloadResultCached(READ_RANGE_VALUES, fileIterator.peek(), readLock);
                    } catch (final NoSuchElementException e) {
                        //end reached
                    }
                }
                try (IFileBufferCacheResult<V> serializingCollection = getResultCached(READ_RANGE_VALUES, value,
                        readLock)) {
                    return serializingCollection.iterator(extractEndTime, from, to);
                }
            }

        };

        final ICloseableIterator<V> rangeValues = new FlatteningIterator<V>(chunkIterator);
        return rangeValues;
    }

    public ICloseableIterator<V> readRangeValuesReverse(final FDate from, final FDate to, final ILock readLock,
            final ISkipFileFunction skipFileFunction) {
        final PeekingCloseableIterator<MemoryFileSummary> fileIterator = new PeekingCloseableIterator<MemoryFileSummary>(
                readRangeFilesReverse(from, to, readLock, skipFileFunction).iterator());
        final ICloseableIterator<ICloseableIterator<V>> chunkIterator = new ATransformingIterator<MemoryFileSummary, ICloseableIterator<V>>(
                fileIterator) {
            @Override
            protected ICloseableIterator<V> transform(final MemoryFileSummary value) {
                if (TimeSeriesProperties.FILE_BUFFER_CACHE_PRELOAD_ENABLED) {
                    try {
                        preloadResultCached(READ_RANGE_VALUES_REVERSE, fileIterator.peek(), readLock);
                    } catch (final NoSuchElementException e) {
                        //end reached
                    }
                }
                try (IFileBufferCacheResult<V> serializingCollection = getResultCached(READ_RANGE_VALUES_REVERSE, value,
                        readLock)) {
                    return serializingCollection.reverseIterator(extractEndTime, from, to);
                }
            }

        };

        final ICloseableIterator<V> rangeValuesReverse = new FlatteningIterator<V>(chunkIterator);
        return rangeValuesReverse;
    }

    private IFileBufferCacheResult<V> getResultCached(final String method, final MemoryFileSummary summary,
            final ILock readLock) {
        return FileBufferCache.getResult(hashKey, summary, newResult(method, summary, readLock));
    }

    private void preloadResultCached(final String method, final MemoryFileSummary summary, final ILock readLock) {
        FileBufferCache.preloadResult(hashKey, summary, newResult(method, summary, readLock));
    }

    private IFileBufferSource<V> newResult(final String method, final MemoryFileSummary summary, final ILock readLock) {
        if (flyweight) {
            final IMemoryMappedFile mmapFile = FileBufferCache.getFile(hashKey, summary.getMemoryResourceUri(), false);
            final MemoryFileSummaryByteBuffer buffer = new MemoryFileSummaryByteBuffer(summary);
            buffer.init(mmapFile);
            return new ByteBufferFileBufferSource<>(buffer, valueSerde, fixedLength);
        } else {
            return new IterableFileBufferSource<V>(newIterableResult(method, summary, readLock), readLock);
        }
    }

    private SerializingCollection<V> newIterableResult(final String method, final MemoryFileSummary summary,
            final ILock readLock) {
        final TextDescription name = new TextDescription("%s[%s]: %s(%s)", TimeSeriesStorageCache.class.getSimpleName(),
                hashKey, method, summary);
        final File memoryFile = new File(summary.getMemoryResourceUri());
        return new SerializingCollection<V>(name, memoryFile, true) {

            @Override
            protected ISerde<V> newSerde() {
                return new FromBufferDelegateSerde<V>(valueSerde);
            }

            @Override
            protected InputStream newFileInputStream(final File file) throws IOException {
                if (TimeSeriesProperties.FILE_BUFFER_CACHE_MMAP_ENABLED) {
                    readLock.lock();
                    final IMemoryMappedFile mmapFile = FileBufferCache.getFile(hashKey, summary.getMemoryResourceUri(),
                            true);
                    if (mmapFile.incrementRefCount()) {
                        return new MmapInputStream(readLock, summary.newBuffer(mmapFile).asInputStream(), mmapFile);
                    } else {
                        readLock.unlock();
                    }
                }
                if (TimeSeriesProperties.FILE_BUFFER_CACHE_SEGMENTS_ENABLED) {
                    readLock.lock();
                    //file buffer cache will close the file quickly
                    final PreLockedBufferedFileDataInputStream in = new PreLockedBufferedFileDataInputStream(readLock,
                            memoryFile);
                    in.position(summary.getMemoryOffset());
                    in.limit(summary.getMemoryOffset() + summary.getMemoryLength());
                    return in;
                } else {
                    //keep file input stream open as shortly as possible to prevent too many open files error
                    readLock.lock();
                    try (BufferedFileDataInputStream in = new BufferedFileDataInputStream(memoryFile)) {
                        in.position(summary.getMemoryOffset());
                        in.limit(summary.getMemoryOffset() + summary.getMemoryLength());
                        final PooledFastByteArrayOutputStream bos = PooledFastByteArrayOutputStream.newInstance();
                        IOUtils.copy(in, bos.asNonClosing());
                        return bos.asInputStream();
                    } catch (final FileNotFoundException e) {
                        //maybe retry because of this in the outer iterator?
                        throw new RetryLaterRuntimeException(
                                hashKey + ": File might have been deleted in the mean time between read locks: "
                                        + file.getAbsolutePath(),
                                e);
                    } finally {
                        readLock.unlock();
                    }
                }
            }

            @Override
            protected Integer newFixedLength() {
                return fixedLength;
            }

            @Override
            protected ICompressionFactory getCompressionFactory() {
                return storage.getCompressionFactory();
            }

            @Override
            protected OutputStream newCompressor(final OutputStream out) {
                return getCompressionFactory().newCompressor(out, ATimeSeriesUpdater.LARGE_COMPRESSOR);
            }

        };
    }

    public V getFirstValue() {
        Optional<V> cachedFirstValueCopy = cachedFirstValue;
        if (cachedFirstValueCopy == null) {
            final ArrayList<? extends RangeTableRow<String, FDate, MemoryFileSummary>> list = getAllRangeKeys(
                    DisabledLock.INSTANCE).getList();
            if (list.isEmpty()) {
                cachedFirstValueCopy = Optional.empty();
            } else {
                final RangeTableRow<String, FDate, MemoryFileSummary> row = list.get(0);
                final MemoryFileSummary latestValue = row.getValue();
                final V firstValue;
                if (latestValue == null) {
                    firstValue = null;
                } else {
                    firstValue = latestValue.getFirstValue(valueSerde);
                }
                cachedFirstValueCopy = Optional.ofNullable(firstValue);
            }
            cachedFirstValue = cachedFirstValueCopy;
        }
        return cachedFirstValueCopy.orElse(null);
    }

    public V getLastValue() {
        Optional<V> cachedLastValueCopy = cachedLastValue;
        if (cachedLastValueCopy == null) {
            final ArrayList<? extends RangeTableRow<String, FDate, MemoryFileSummary>> list = getAllRangeKeys(
                    DisabledLock.INSTANCE).getList();
            if (list.isEmpty()) {
                cachedLastValueCopy = Optional.empty();
            } else {
                final RangeTableRow<String, FDate, MemoryFileSummary> row = list.get(list.size() - 1);
                final MemoryFileSummary latestValue = row.getValue();
                final V lastValue;
                if (latestValue == null) {
                    lastValue = null;
                } else {
                    lastValue = latestValue.getLastValue(valueSerde);
                }
                cachedLastValueCopy = Optional.ofNullable(lastValue);
            }
            cachedLastValue = cachedLastValueCopy;
        }
        return cachedLastValueCopy.orElse(null);
    }

    public synchronized void deleteAll() {
        storage.getFileLookupTable().deleteRange(hashKey);
        storage.deleteRange_latestValueLookupTable(hashKey);
        storage.deleteRange_nextValueLookupTable(hashKey);
        storage.deleteRange_previousValueLookupTable(hashKey);
        clearCaches();
        Files.deleteNative(newDataDirectory());
        memoryFileMetadata = null;
        dataDirectory = null;
    }

    private void clearCaches() {
        fileLookupTable_latestRangeKeyCache.clear();
        fileLookupTable_latestRangeKeyIndexCache.clear();
        FileBufferCache.remove(hashKey);
        cachedAllRangeKeys.set(null);
        cachedFirstValue = null;
        cachedLastValue = null;
        cachedSize = -1L;
        latestValueIndexLookupCache.clear();
        nextValueIndexLookupCache.clear();
        previousValueIndexLookupCache.clear();
        lastResetIndex++;
    }

    public V getLatestValue(final FDate date) {
        switch (lookupMode) {
        case Value:
            return getLatestValueByValue(date);
        case ValueUntilIndexAvailable:
        case Index:
            return getLatestValueByIndex(date);
        default:
            throw UnknownArgumentException.newInstance(TimeSeriesLookupMode.class, lookupMode);
        }
    }

    private V getLatestValueByIndex(final FDate date) {
        final ALatestValueByIndexCache<V> latestValueByIndexCache = latestValueByIndexCacheHolder.get();
        return latestValueByIndexCache.getLatestValueByIndex(date);
    }

    public long getLatestValueIndex(final FDate date) {
        final long valueIndex = latestValueIndexLookupCache.get(date);
        return valueIndex;
    }

    private long latestValueIndexLookup(final FDate date) {
        final RangeTableRow<String, FDate, MemoryFileSummary> row = getLatestRangeKey(date);
        if (row == null) {
            return -1L;
        }
        final MemoryFileSummary summary = row.getValue();
        if (summary == null) {
            return -1L;
        }
        try (IFileBufferCacheResult<V> result = getResultCached("latestValueLookupCache.loadValue", summary,
                DisabledLock.INSTANCE)) {
            final int latestValueIndex = result.getLatestValueIndex(extractEndTime, date);
            if (latestValueIndex == -1 && getFirstValue() != null) {
                return 0L;
            }
            if (latestValueIndex == -1) {
                return -1L;
            }
            return row.getValue().getPrecedingValueCount() + latestValueIndex;
        }
    }

    public V getLatestValue(final long index) {
        if (index >= size() - 1) {
            return getLastValue();
        }
        if (index <= 0) {
            return getFirstValue();
        }
        final RangeTableRow<String, FDate, MemoryFileSummary> row = getLatestRangeKeyIndex(index);
        if (row == null) {
            return null;
        }
        final MemoryFileSummary summary = row.getValue();
        if (summary == null) {
            return null;
        }
        try (IFileBufferCacheResult<V> result = getResultCached("latestValueLookupCache.loadValue", summary,
                DisabledLock.INSTANCE)) {
            final long rowIndex = index - row.getValue().getPrecedingValueCount();
            final V latestValue = result.getLatestValue(Integers.checkedCast(rowIndex));
            if (latestValue == null) {
                return getFirstValue();
            }
            return latestValue;
        }
    }

    private V getLatestValueByValue(final FDate date) {
        final SingleValue value = storage.getOrLoad_latestValueLookupTable(hashKey, date, () -> {
            final RangeTableRow<String, FDate, MemoryFileSummary> row = fileLookupTable_latestRangeKeyCache.get(date);
            if (row == null) {
                return null;
            }
            final MemoryFileSummary summary = row.getValue();
            if (summary == null) {
                return null;
            }
            try (IFileBufferCacheResult<V> result = getResultCached("latestValueLookupCache.loadValue", summary,
                    DisabledLock.INSTANCE)) {
                V latestValue = result.getLatestValue(extractEndTime, date);
                if (latestValue == null) {
                    latestValue = getFirstValue();
                }
                if (latestValue == null) {
                    return null;
                }
                return new SingleValue(valueSerde, latestValue);
            }
        });
        if (value == null) {
            return null;
        }
        return value.getValue(valueSerde);
    }

    public long size() {
        long cachedSizeCopy = cachedSize;
        if (cachedSizeCopy == -1L) {
            final ArrayList<? extends RangeTableRow<String, FDate, MemoryFileSummary>> list = getAllRangeKeys(
                    DisabledLock.INSTANCE).getList();
            if (list.isEmpty()) {
                cachedSizeCopy = 0;
            } else {
                long size = 0;
                for (int i = 0; i < list.size(); i++) {
                    size += list.get(i).getValue().getValueCount();
                }
                cachedSizeCopy = size;
            }
            cachedSize = cachedSizeCopy;
        }
        return cachedSizeCopy;
    }

    public V getPreviousValue(final FDate date, final int shiftBackUnits) {
        switch (lookupMode) {
        case Value:
            return getPreviousValueByValue(date, shiftBackUnits);
        case ValueUntilIndexAvailable:
        case Index:
            return getPreviousValueByIndex(date, shiftBackUnits);
        default:
            throw UnknownArgumentException.newInstance(TimeSeriesLookupMode.class, lookupMode);
        }
    }

    private V getPreviousValueByIndex(final FDate date, final int shiftBackUnits) {
        assertShiftUnitsPositiveNonZero(shiftBackUnits);
        final V firstValue = getFirstValue();
        if (firstValue == null) {
            return null;
        }
        final FDate firstTime = extractEndTime.apply(firstValue);
        if (date.isBeforeOrEqualTo(firstTime)) {
            return firstValue;
        } else {
            final long valueIndex = previousValueIndexLookupCache.get(new RangeShiftUnitsKey(date, shiftBackUnits));
            return getLatestValue(valueIndex);
        }
    }

    private long previousValueIndexLookup(final FDate date, final int shiftBackUnits) {
        final AShiftBackUnitsLoopLongIndex<V> shiftBackLoop = new AShiftBackUnitsLoopLongIndex<V>(date,
                shiftBackUnits) {
            @Override
            protected V getLatestValue(final long index) {
                return TimeSeriesStorageCache.this.getLatestValue(index);
            }

            @Override
            protected long getLatestValueIndex(final FDate date) {
                return TimeSeriesStorageCache.this.getLatestValueIndex(date);
            }

            @Override
            protected FDate extractEndTime(final V value) {
                return extractEndTime.apply(value);
            }

            @Override
            protected long size() {
                return TimeSeriesStorageCache.this.size();
            }
        };
        shiftBackLoop.loop();
        return shiftBackLoop.getPrevValueIndex();
    }

    private V getPreviousValueByValue(final FDate date, final int shiftBackUnits) {
        assertShiftUnitsPositiveNonZero(shiftBackUnits);
        final V firstValue = getFirstValue();
        if (firstValue == null) {
            return null;
        }
        final FDate firstTime = extractEndTime.apply(firstValue);
        if (date.isBeforeOrEqualTo(firstTime)) {
            return firstValue;
        } else {
            final SingleValue value = storage.getOrLoad_previousValueLookupTable(hashKey, date, shiftBackUnits, () -> {
                final ShiftBackUnitsLoop<V> shiftBackLoop = new ShiftBackUnitsLoop<>(date, shiftBackUnits,
                        extractEndTime);
                final ICloseableIterator<V> rangeValuesReverse = readRangeValuesReverse(date, null,
                        DisabledLock.INSTANCE, file -> {
                            final boolean skip = shiftBackLoop.getPrevValue() != null
                                    && file.getValueCount() < shiftBackLoop.getShiftBackRemaining();
                            if (skip) {
                                shiftBackLoop.skip(file.getValueCount());
                            }
                            return skip;
                        });
                shiftBackLoop.loop(rangeValuesReverse);
                return new SingleValue(valueSerde, shiftBackLoop.getPrevValue());
            });
            return value.getValue(valueSerde);
        }

    }

    public V getNextValue(final FDate date, final int shiftForwardUnits) {
        switch (lookupMode) {
        case Value:
            return getNextValueByValue(date, shiftForwardUnits);
        case ValueUntilIndexAvailable:
        case Index:
            return getNextValueByIndex(date, shiftForwardUnits);
        default:
            throw UnknownArgumentException.newInstance(TimeSeriesLookupMode.class, lookupMode);
        }
    }

    private V getNextValueByIndex(final FDate date, final int shiftForwardUnits) {
        assertShiftUnitsPositiveNonZero(shiftForwardUnits);
        final V lastValue = getLastValue();
        if (lastValue == null) {
            return null;
        }
        final FDate lastTime = extractEndTime.apply(lastValue);
        if (date.isAfterOrEqualTo(lastTime)) {
            return lastValue;
        } else {
            final long valueIndex = nextValueIndexLookupCache.get(new RangeShiftUnitsKey(date, shiftForwardUnits));
            return getLatestValue(valueIndex);
        }
    }

    private long nextValueIndexLookup(final FDate date, final int shiftForwardUnits) {
        final AShiftForwardUnitsLoopLongIndex<V> shiftForwardLoop = new AShiftForwardUnitsLoopLongIndex<V>(date,
                shiftForwardUnits) {
            @Override
            protected V getLatestValue(final long index) {
                return TimeSeriesStorageCache.this.getLatestValue(index);
            }

            @Override
            protected long getLatestValueIndex(final FDate date) {
                return TimeSeriesStorageCache.this.getLatestValueIndex(date);
            }

            @Override
            protected FDate extractEndTime(final V value) {
                return extractEndTime.apply(value);
            }

            @Override
            protected long size() {
                return TimeSeriesStorageCache.this.size();
            }
        };
        shiftForwardLoop.loop();
        return shiftForwardLoop.getNextValueIndex();
    }

    private V getNextValueByValue(final FDate date, final int shiftForwardUnits) {
        assertShiftUnitsPositiveNonZero(shiftForwardUnits);
        final V lastValue = getLastValue();
        if (lastValue == null) {
            return null;
        }
        final FDate lastTime = extractEndTime.apply(lastValue);
        if (date.isAfterOrEqualTo(lastTime)) {
            return lastValue;
        } else {
            final SingleValue value = storage.getOrLoad_nextValueLookupTable(hashKey, date, shiftForwardUnits, () -> {
                final ShiftForwardUnitsLoop<V> shiftForwardLoop = new ShiftForwardUnitsLoop<>(date, shiftForwardUnits,
                        extractEndTime);
                final ICloseableIterator<V> rangeValues = readRangeValues(date, null, DisabledLock.INSTANCE,
                        new ISkipFileFunction() {
                            @Override
                            public boolean skipFile(final MemoryFileSummary file) {
                                final boolean skip = shiftForwardLoop.getNextValue() != null
                                        && file.getValueCount() < shiftForwardLoop.getShiftForwardRemaining();
                                if (skip) {
                                    shiftForwardLoop.skip(file.getValueCount());
                                }
                                return skip;
                            }
                        });
                shiftForwardLoop.loop(rangeValues);
                return new SingleValue(valueSerde, shiftForwardLoop.getNextValue());
            });
            return value.getValue(valueSerde);
        }
    }

    public boolean isEmptyOrInconsistent() {
        try {
            getFirstValue();
            getLastValue();
        } catch (final Throwable t) {
            if (Throwables.isCausedByType(t, SerializationException.class)) {
                //e.g. fst: unable to find class for code 88 after version upgrade
                log.warn("Table data for [%s] is inconsistent and needs to be reset. Exception during getLastValue: %s",
                        hashKey, t.toString());
                return true;
            } else {
                //unexpected exception, since RemoteFastSerializingSerde only throws SerializingException
                throw Throwables.propagate(t);
            }
        }
        final MemoryFileMetadata metadata = getMemoryFileMetadata();
        final long expectedMemoryFileSize = metadata.getExpectedMemoryFileSize();
        long calculatedMemoryFileSize = 0;
        long actualMemoryFileSize = 0;
        MemoryFileSummary prevSummary = null;
        File prevMemoryFile = null;

        try (ICloseableIterator<MemoryFileSummary> summaries = readRangeFiles(null, null, DisabledLock.INSTANCE, null)
                .iterator()) {
            boolean noFileFound = true;
            while (summaries.hasNext()) {
                final MemoryFileSummary summary = summaries.next();
                final File memoryFile = new File(summary.getMemoryResourceUri());
                final long memoryFileLength = memoryFile.length();
                calculatedMemoryFileSize = summary.getPrecedingMemoryOffset() + summary.getMemoryOffset()
                        + summary.getMemoryLength();
                if (!Objects.equals(prevMemoryFile, memoryFile)) {
                    actualMemoryFileSize += memoryFileLength;
                }
                if (calculatedMemoryFileSize > actualMemoryFileSize) {
                    log.warn("Table data for [%s] is inconsistent and needs to be reset. Empty file: [%s]", hashKey,
                            summary);
                    return true;
                }
                try {
                    assertSummary(prevSummary, summary);
                } catch (final Throwable t) {
                    log.warn(
                            "Table data for [%s] is inconsistent and needs to be reset. Inconsistent summary file [%s]: [%s]",
                            hashKey, t.toString(), summary);
                    return true;
                }
                prevSummary = summary;
                prevMemoryFile = memoryFile;
                noFileFound = false;
            }
            if (noFileFound) {
                return true;
            }
            if (expectedMemoryFileSize != MemoryFileMetadata.MISSING_EXPECTED_MEMORY_FILE_SIZE) {
                if (expectedMemoryFileSize != calculatedMemoryFileSize) {
                    log.warn(
                            "Table data for [%s] is inconsistent and needs to be reset. ExpectedMemoryFileSize[%s] != CalculatedMemoryFileSize[%s]",
                            hashKey, expectedMemoryFileSize, calculatedMemoryFileSize);
                    return true;
                }
                if (actualMemoryFileSize != expectedMemoryFileSize) {
                    log.warn(
                            "Table data for [%s] is inconsistent and needs to be reset. ActualMemoryFileSize[%s] != ExpectedMemoryFileSize[%s]",
                            hashKey, actualMemoryFileSize, expectedMemoryFileSize);
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * When shouldRedoLastFile=true this deletes the last file in order to create a new updated one (so the files do not
     * get fragmented too much between updates
     */
    public synchronized PrepareForUpdateResult<V> prepareForUpdate(final boolean shouldRedoLastFile) {
        final RangeTableRow<String, FDate, MemoryFileSummary> latestFile = storage.getFileLookupTable()
                .getLatest(hashKey, FDates.MAX_DATE);
        final FDate updateFrom;
        final List<V> lastValues;
        final long precedingMemoryOffset;
        final long memoryOffset;
        final long precedingValueCount;
        if (latestFile != null) {
            final FDate latestRangeKey;
            final MemoryFileSummary latestSummary = latestFile.getValue();
            if (shouldRedoLastFile && latestSummary.getValueCount() < ATimeSeriesUpdater.DEFAULT_BATCH_FLUSH_INTERVAL) {
                lastValues = new ArrayList<V>();
                try (ICloseableIterator<V> lastColl = newIterableResult("prepareForUpdate", latestSummary,
                        DisabledLock.INSTANCE).iterator()) {
                    Lists.toListWithoutHasNext(lastColl, lastValues);
                }
                if (!lastValues.isEmpty()) {
                    //remove last value because it might be an incomplete bar
                    final V lastValue = lastValues.remove(lastValues.size() - 1);
                    precedingMemoryOffset = latestSummary.getPrecedingMemoryOffset();
                    memoryOffset = latestSummary.getMemoryOffset();
                    precedingValueCount = latestSummary.getPrecedingValueCount();
                    updateFrom = extractEndTime.apply(lastValue);
                    latestRangeKey = latestFile.getRangeKey();
                } else {
                    precedingMemoryOffset = latestSummary.getPrecedingMemoryOffset();
                    memoryOffset = latestSummary.getMemoryOffset() + latestSummary.getMemoryLength() + 1L;
                    precedingValueCount = latestSummary.getPrecedingValueCount() + latestSummary.getValueCount();
                    updateFrom = latestFile.getRangeKey();
                    latestRangeKey = latestFile.getRangeKey().addMilliseconds(1);
                }
            } else {
                lastValues = Collections.emptyList();
                precedingMemoryOffset = latestSummary.getPrecedingMemoryOffset();
                memoryOffset = latestSummary.getMemoryOffset() + latestSummary.getMemoryLength() + 1L;
                precedingValueCount = latestSummary.getPrecedingValueCount() + latestSummary.getValueCount();
                updateFrom = latestFile.getRangeKey();
                latestRangeKey = latestFile.getRangeKey().addMilliseconds(1);
            }
            storage.getFileLookupTable().deleteRange(hashKey, latestRangeKey);
            storage.deleteRange_latestValueLookupTable(hashKey, latestRangeKey);
            storage.deleteRange_nextValueLookupTable(hashKey); //we cannot be sure here about the date since shift keys can be arbitrarily large
            storage.deleteRange_previousValueLookupTable(hashKey, latestRangeKey);
            latestValueIndexLookupCache.clear();
            nextValueIndexLookupCache.clear();
            previousValueIndexLookupCache.clear();
            lastResetIndex++;
        } else {
            updateFrom = null;
            lastValues = Collections.emptyList();
            precedingMemoryOffset = 0L;
            memoryOffset = 0L;
            precedingValueCount = 0L;
        }
        clearCaches();
        return new PrepareForUpdateResult<>(updateFrom, lastValues, precedingMemoryOffset, memoryOffset,
                precedingValueCount);
    }

    private void assertShiftUnitsPositiveNonZero(final int shiftUnits) {
        if (shiftUnits < 0) {
            throw new IllegalArgumentException("shiftUnits needs to be a positive or zero value: " + shiftUnits);
        }
    }

    private ArrayFileBufferCacheResult<RangeTableRow<String, FDate, MemoryFileSummary>> getAllRangeKeys(
            final ILock readLock) {
        ArrayFileBufferCacheResult<RangeTableRow<String, FDate, MemoryFileSummary>> cachedAllRangeKeysCopy = cachedAllRangeKeys
                .get();
        if (cachedAllRangeKeysCopy == null) {
            readLock.lock();
            try {
                cachedAllRangeKeysCopy = cachedAllRangeKeys.get();
                if (cachedAllRangeKeysCopy == null) {
                    try (ICloseableIterator<RangeTableRow<String, FDate, MemoryFileSummary>> range = storage
                            .getFileLookupTable()
                            .range(hashKey, FDates.MIN_DATE, FDates.MAX_DATE)) {
                        final ArrayList<RangeTableRow<String, FDate, MemoryFileSummary>> allRangeKeys = new ArrayList<>();
                        Lists.toListWithoutHasNext(range, allRangeKeys);
                        cachedAllRangeKeysCopy = new ArrayFileBufferCacheResult<RangeTableRow<String, FDate, MemoryFileSummary>>(
                                allRangeKeys);
                        cachedAllRangeKeys.set(cachedAllRangeKeysCopy);
                    }
                }
            } finally {
                readLock.unlock();
            }
        }
        return cachedAllRangeKeysCopy;
    }

    private final class LatestValueByIndexCache extends ALatestValueByIndexCache<V> {
        @Override
        protected long getLatestValueIndex(final FDate key) {
            return TimeSeriesStorageCache.this.getLatestValueIndex(key);
        }

        @Override
        protected V getLatestValue(final long index) {
            return TimeSeriesStorageCache.this.getLatestValue(index);
        }

        @Override
        protected FDate extractEndTime(final V value) {
            return extractEndTime.apply(value);
        }

        @Override
        protected int getLastResetIndex() {
            return lastResetIndex;
        }
    }

    private static final class MmapInputStream extends PreLockedDelegateInputStream {
        private IMemoryMappedFile mmapFile;

        private MmapInputStream(final Lock lock, final InputStream delegate, final IMemoryMappedFile mmapFile) {
            super(lock, delegate);
            this.mmapFile = mmapFile;
        }

        @Override
        public void close() throws IOException {
            if (mmapFile != null) {
                super.close();
                mmapFile.decrementRefCount();
                mmapFile = null;
            }
        }
    }

}
