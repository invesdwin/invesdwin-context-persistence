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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SerializationException;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;

import de.invesdwin.context.integration.compression.DisabledCompressionFactory;
import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.filechannel.IFileChannel;
import de.invesdwin.context.integration.filechannel.registry.FileChannelRegistry;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.timeseriesdb.buffer.ArrayFileBufferCacheResult;
import de.invesdwin.context.persistence.timeseriesdb.buffer.FileBufferCache;
import de.invesdwin.context.persistence.timeseriesdb.buffer.IFileBufferCacheResult;
import de.invesdwin.context.persistence.timeseriesdb.buffer.source.ByteBufferFileBufferSource;
import de.invesdwin.context.persistence.timeseriesdb.buffer.source.IFileBufferSource;
import de.invesdwin.context.persistence.timeseriesdb.buffer.source.IterableFileBufferSource;
import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.ITimeSeriesDirectoryHashKey;
import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.TimeSeriesDirectoryHashKey;
import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.data.TimeSeriesDirectoryHashKeyVersionData;
import de.invesdwin.context.persistence.timeseriesdb.loop.AShiftBackUnitsLoopLongIndex;
import de.invesdwin.context.persistence.timeseriesdb.loop.AShiftForwardUnitsLoopLongIndex;
import de.invesdwin.context.persistence.timeseriesdb.storage.SingleValue;
import de.invesdwin.context.persistence.timeseriesdb.storage.TimeSeriesStorage;
import de.invesdwin.context.persistence.timeseriesdb.storage.cache.ALatestValueByIndexCache;
import de.invesdwin.context.persistence.timeseriesdb.storage.key.RangeShiftUnitsKey;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.ISkipMemoryFileSummaryFunction;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummaryByteBuffer;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFiles;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.ITimeSeriesMemoryFileLookupTable;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.MemoryFileMetadata;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.RefreshingTimeSeriesMemoryFileLookupTable;
import de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.context.persistence.timeseriesdb.updater.TimeSeriesUpdateTransaction;
import de.invesdwin.context.system.properties.ICloseableProperties;
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
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import de.invesdwin.util.collections.loadingcache.ILoadingCache;
import de.invesdwin.util.collections.loadingcache.historical.query.impl.ShiftBackUnitsLoop;
import de.invesdwin.util.collections.loadingcache.historical.query.impl.ShiftForwardUnitsLoop;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.disabled.DisabledLock;
import de.invesdwin.util.concurrent.reference.MutableSoftReference;
import de.invesdwin.util.concurrent.reference.WeakThreadLocalReference;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.marshallers.serde.FromBufferDelegateSerde;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.math.Longs;
import de.invesdwin.util.streams.buffer.file.IMemoryMappedFile;
import de.invesdwin.util.streams.buffer.memory.delegate.SegmentedMemoryBuffer;
import de.invesdwin.util.streams.delegate.SimpleDelegateInputStream;
import de.invesdwin.util.streams.pool.PooledFastByteArrayOutputStream;
import de.invesdwin.util.streams.pool.buffered.BufferedFileDataInputStream;
import de.invesdwin.util.streams.pool.buffered.PreLockedBufferedFileDataInputStream;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FTimeUnit;

@NotThreadSafe
public class TimeSeriesLookupStorageCache<K, V> {
    public static final Integer MAXIMUM_SIZE = TimeSeriesProperties.STORAGE_CACHE_MAXIMUM_SIZE;
    public static final EvictionMode EVICTION_MODE = EvictionMode.ClearConcurrent;
    public static final boolean HIGH_CONCURRENCY = false;

    private static final String READ_RANGE_VALUES = "readRangeValues";
    private static final String READ_RANGE_VALUES_REVERSE = "readRangeValuesReverse";
    private final TimeSeriesStorage storage;
    private final RefreshingTimeSeriesMemoryFileLookupTable<V> memoryFileLookupTable;
    private final ILoadingCache<FDate, Long> latestValueIndexLookupCache = new ALoadingCache<FDate, Long>() {

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
            return HIGH_CONCURRENCY;
        }

        @Override
        protected Long loadValue(final FDate key) {
            return latestValueIndexLookup(key);
        }
    };
    private final ILoadingCache<RangeShiftUnitsKey, Long> previousValueIndexLookupCache = new ALoadingCache<RangeShiftUnitsKey, Long>() {

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
            return HIGH_CONCURRENCY;
        }

        @Override
        protected Long loadValue(final RangeShiftUnitsKey key) {
            return previousValueIndexLookup(key.getRangeKey(), key.getShiftUnits());
        }
    };
    private final ILoadingCache<RangeShiftUnitsKey, Long> nextValueIndexLookupCache = new ALoadingCache<RangeShiftUnitsKey, Long>() {

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
            return HIGH_CONCURRENCY;
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
    private final AtomicInteger lastResetIndex = new AtomicInteger();

    private final String hashKey;
    private final ISerde<V> valueSerde;
    private final Integer valueFixedLength;
    private final ICompressionFactory compressionFactory;
    private final Function<V, FDate> extractEndTime;
    private final boolean flyweight;
    private final TimeSeriesLookupMode lookupMode;
    private final int batchFlushInterval;

    private volatile Optional<V> cachedFirstValue;
    private volatile Optional<V> cachedLastValue;
    private volatile long cachedSize = -1L;
    /**
     * keeping the range keys outside of the concurrent linked hashmap of the ADelegateRangeTable with memory write
     * through to disk is still better for increased parallelity and for not having to iterate through each element of
     * the other hashkeys.
     */
    private final MutableSoftReference<ArrayFileBufferCacheResult<MemoryFileSummary>> cachedAllRangeKeys = new MutableSoftReference<ArrayFileBufferCacheResult<MemoryFileSummary>>(
            null);
    private final Log log = new Log(this);
    private final LoadingCache<ResultCacheKey, IFileBufferCacheResult<V>> resultCache;
    private final ITimeSeriesDirectoryHashKey directoryHashKey;
    private final TimeSeriesDirectoryHashKeyVersionData directoryHashKeyVersionMemory;

    public TimeSeriesLookupStorageCache(final TimeSeriesStorage storage, final String hashKey,
            final ISerde<V> valueSerde, final Integer valueFixedLength, final ICompressionFactory compressionFactory,
            final Function<V, FDate> extractEndTime, final TimeSeriesLookupMode lookupMode,
            final int batchFlushInterval) {
        this.storage = storage;
        this.hashKey = hashKey;
        this.directoryHashKey = new TimeSeriesDirectoryHashKey(storage.getDirectory(), hashKey);
        this.directoryHashKeyVersionMemory = new TimeSeriesDirectoryHashKeyVersionData(
                directoryHashKey.getDirectoryHashKeyVersion(), "memory");
        this.memoryFileLookupTable = new RefreshingTimeSeriesMemoryFileLookupTable<V>(this,
                directoryHashKeyVersionMemory);
        this.valueSerde = valueSerde;
        this.valueFixedLength = valueFixedLength;
        this.compressionFactory = compressionFactory;
        this.extractEndTime = extractEndTime;
        final boolean compressed = storage.getCompressionFactory() != DisabledCompressionFactory.INSTANCE;
        final boolean mmap = TimeSeriesProperties.FILE_BUFFER_CACHE_MMAP_ENABLED;
        this.flyweight = !compressed && mmap && valueFixedLength != null && valueFixedLength > 0;
        this.lookupMode = lookupMode;
        this.batchFlushInterval = batchFlushInterval;
        this.resultCache = Caffeine.newBuilder()
                .maximumSize(TimeSeriesProperties.FILE_BUFFER_CACHE_MIN_SEGMENTS_COUNT)
                /*
                 * evict more aggressively than the file buffer cache so that file buffer cache results are kept alive
                 * longer to keep the really hot segments longer in memory
                 */
                .weakValues()
                .expireAfterAccess(
                        TimeSeriesProperties.FILE_BUFFER_CACHE_EVICTION_TIMEOUT.longValue(FTimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .removalListener(this::resultCache_onRemoval)
                .<ResultCacheKey, IFileBufferCacheResult<V>> build(this::resultCache_load);
    }

    public int getBatchFlushInterval() {
        return batchFlushInterval;
    }

    private IFileBufferCacheResult<V> resultCache_load(final ResultCacheKey key) throws Exception {
        final IFileBufferCacheResult<V> result = FileBufferCache.getResult(hashKey, key.getSummary(), key.getSource());
        if (result instanceof ArrayFileBufferCacheResult) {
            final ArrayFileBufferCacheResult<?> cResult = (ArrayFileBufferCacheResult<?>) result;
            cResult.getRefCount().incrementAndGet();
        }
        return result;
    }

    private void resultCache_onRemoval(final ResultCacheKey key, final IFileBufferCacheResult<V> value,
            final RemovalCause cause) {
        if (value instanceof ArrayFileBufferCacheResult) {
            final ArrayFileBufferCacheResult<?> cValue = (ArrayFileBufferCacheResult<?>) value;
            cValue.getRefCount().decrementAndGet();
        }
    }

    public ISerde<V> getValueSerde() {
        return valueSerde;
    }

    public Integer getValueFixedLength() {
        return valueFixedLength;
    }

    public ICompressionFactory getCompressionFactory() {
        return compressionFactory;
    }

    public FDate extractEndTime(final V value) {
        return extractEndTime.apply(value);
    }

    public ITimeSeriesDirectoryHashKey getDirectoryHashKey() {
        return directoryHashKey;
    }

    public TimeSeriesDirectoryHashKeyVersionData getDirectoryHashKeyVersionMemory() {
        return directoryHashKeyVersionMemory;
    }

    public ITimeSeriesMemoryFileLookupTable getMemoryFileLookupTable() {
        return memoryFileLookupTable;
    }

    public File getUpdateLockFile() {
        return new File(directoryHashKeyVersionMemory.getDirectoryHashKeyVersionDataShared(), "updateRunning.lock");
    }

    public MemoryFileSummary getLastRangeKey() {
        final List<MemoryFileSummary> list = getAllRangeKeys(DisabledLock.INSTANCE).getList();
        final MemoryFileSummary prevSummary;
        if (list.isEmpty()) {
            prevSummary = null;
        } else {
            prevSummary = list.get(list.size() - 1);
        }
        return prevSummary;
    }

    protected ICloseableIterable<MemoryFileSummary> readRangeFiles(final FDate from, final FDate to,
            final ILock readLock, final ISkipMemoryFileSummaryFunction skipFileFunction) {
        return new ICloseableIterable<MemoryFileSummary>() {

            @Override
            public ICloseableIterator<MemoryFileSummary> iterator() {
                final FDate usedFrom;
                if (from == null) {
                    final V firstValue = getFirstValue();
                    if (firstValue == null) {
                        return EmptyCloseableIterator.getInstance();
                    }
                    usedFrom = extractEndTime(firstValue);
                } else {
                    usedFrom = from;
                }
                return new ACloseableIterator<MemoryFileSummary>(new TextDescription("%s[%s]: readRangeFiles(%s, %s)",
                        TimeSeriesLookupStorageCache.class.getSimpleName(), hashKey, from, to)) {

                    //use latest time available even if delegate iterator has no values
                    private MemoryFileSummary latestFirstTime = getLatestRangeKey(usedFrom);
                    private final ICloseableIterator<MemoryFileSummary> delegate;

                    {
                        if (latestFirstTime == null) {
                            delegate = EmptyCloseableIterator.getInstance();
                        } else {
                            delegate = getRangeKeys(latestFirstTime.getFirstValueEndTime().addPicoseconds(1), to);
                        }
                    }

                    @Override
                    protected boolean innerHasNext() {
                        return latestFirstTime != null || delegate.hasNext();
                    }

                    private ICloseableIterator<MemoryFileSummary> getRangeKeys(final FDate from, final FDate to) {
                        final ArrayFileBufferCacheResult<MemoryFileSummary> rangeSource = getAllRangeKeys(readLock);
                        final ICloseableIterator<MemoryFileSummary> rangeFiltered = rangeSource
                                .iterator(MemoryFileSummary::getFirstValueEndTime, from, to);
                        if (skipFileFunction != null) {
                            return new ASkippingIterator<MemoryFileSummary>(rangeFiltered) {
                                @Override
                                protected boolean skip(final MemoryFileSummary element) {
                                    if (!rangeFiltered.hasNext()) {
                                        /*
                                         * cannot optimize this further for multiple segments because we don't know if a
                                         * segment further back might be empty or not and thus the last segment of
                                         * interest might have been the previous one from which we skipped the last file
                                         * falsely
                                         */
                                        return false;
                                    }
                                    return skipFileFunction.skipFile(element);
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
                            summary = latestFirstTime;
                            latestFirstTime = null;
                        } else {
                            summary = delegate.next();
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

    private MemoryFileSummary getLatestRangeKey(final FDate key) {
        final ArrayFileBufferCacheResult<MemoryFileSummary> allRangeKeys = getAllRangeKeys(DisabledLock.INSTANCE);
        return allRangeKeys.getLatestValue(MemoryFileSummary::getFirstValueEndTime, key);
    }

    private MemoryFileSummary getLatestRangeKeyIndex(final long key) {
        return newLatestRangeKeyIndex(key);
    }

    private MemoryFileSummary newLatestRangeKeyIndex(final long key) {
        final ArrayList<MemoryFileSummary> rows = getAllRangeKeys(DisabledLock.INSTANCE).getList();
        if (rows.isEmpty()) {
            return null;
        }
        final MemoryFileSummary firstRow = rows.get(0);
        if (key <= 0) {
            return firstRow;
        }
        final int segmentSize = firstRow.getValueCount();
        final int segmentIndex = SegmentedMemoryBuffer.getSegmentIndex(key, segmentSize);
        if (segmentIndex >= rows.size()) {
            return rows.get(rows.size() - 1);
        } else {
            final MemoryFileSummary row = rows.get(segmentIndex);
            if (row.getPrecedingValueCount() <= key && key < row.getCombinedValueCount()) {
                return row;
            }
            throw new IllegalStateException("key [" + key + "] should be in the key range of the returned row ["
                    + row.getPrecedingValueCount() + " to " + row.getCombinedValueCount() + "]: " + row);
        }
    }

    protected ICloseableIterable<MemoryFileSummary> readRangeFilesReverse(final FDate from, final FDate to,
            final ILock readLock, final ISkipMemoryFileSummaryFunction skipFileFunction) {
        return new ICloseableIterable<MemoryFileSummary>() {

            @Override
            public ICloseableIterator<MemoryFileSummary> iterator() {
                final FDate usedFrom;
                if (from == null) {
                    final V lastValue = getLastValue();
                    if (lastValue == null) {
                        return EmptyCloseableIterator.getInstance();
                    }
                    usedFrom = extractEndTime(lastValue);
                } else {
                    usedFrom = from;
                }
                return new ACloseableIterator<MemoryFileSummary>(
                        new TextDescription("%s[%s]: readRangeFilesReverse(%s, %s)",
                                TimeSeriesLookupStorageCache.class.getSimpleName(), hashKey, from, to)) {

                    //use latest time available even if delegate iterator has no values
                    private MemoryFileSummary latestLastTime = getLatestRangeKey(usedFrom);
                    // add 1 ms to not collide with firstTime
                    private final ICloseableIterator<MemoryFileSummary> delegate;

                    {
                        if (latestLastTime == null) {
                            delegate = EmptyCloseableIterator.getInstance();
                        } else {
                            delegate = getRangeKeysReverse(latestLastTime.getFirstValueEndTime().addPicoseconds(-1),
                                    to);
                        }
                    }

                    @Override
                    protected boolean innerHasNext() {
                        return latestLastTime != null || delegate.hasNext();
                    }

                    private ICloseableIterator<MemoryFileSummary> getRangeKeysReverse(final FDate from,
                            final FDate to) {
                        final ArrayFileBufferCacheResult<MemoryFileSummary> rangeSource = getAllRangeKeys(readLock);
                        final ICloseableIterator<MemoryFileSummary> rangeFiltered = rangeSource
                                .reverseIterator(MemoryFileSummary::getFirstValueEndTime, from, to);
                        if (skipFileFunction != null) {
                            return new ASkippingIterator<MemoryFileSummary>(rangeFiltered) {

                                @Override
                                protected boolean skip(final MemoryFileSummary element) {
                                    if (!rangeFiltered.hasNext()) {
                                        /*
                                         * cannot optimize this further for multiple segments because we don't know if a
                                         * segment further back might be empty or not and thus the last segment of
                                         * interest might have been the previous one from which we skipped the last file
                                         * falsely
                                         */
                                        return false;
                                    }
                                    return skipFileFunction.skipFile(element);
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
                            summary = latestLastTime;
                            latestLastTime = null;
                        } else {
                            summary = delegate.next();
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
            final ISkipMemoryFileSummaryFunction skipFileFunction) {
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
            final ISkipMemoryFileSummaryFunction skipFileFunction) {
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
        //        return FileBufferCache.getResult(hashKey, summary, newResult(method, summary, readLock));
        return resultCache.get(new ResultCacheKey(summary, newResult(method, summary, readLock)));
    }

    private void preloadResultCached(final String method, final MemoryFileSummary summary, final ILock readLock) {
        FileBufferCache.preloadResult(hashKey, summary, newResult(method, summary, readLock));
    }

    private IFileBufferSource<V> newResult(final String method, final MemoryFileSummary summary, final ILock readLock) {
        if (flyweight) {
            final IMemoryMappedFile mmapFile = FileBufferCache.getFile(hashKey, summary.getMemoryResourceUri(), false);
            final MemoryFileSummaryByteBuffer buffer = new MemoryFileSummaryByteBuffer(summary);
            buffer.init(mmapFile);
            return new ByteBufferFileBufferSource<>(buffer, valueSerde, valueFixedLength);
        } else {
            return new IterableFileBufferSource<V>(newIterableResult(method, summary, readLock), readLock);
        }
    }

    private SerializingCollection<V> newIterableResult(final String method, final MemoryFileSummary summary,
            final ILock readLock) {
        final TextDescription name = new TextDescription("%s[%s]: %s(%s)",
                TimeSeriesLookupStorageCache.class.getSimpleName(), hashKey, method, summary);
        final File memoryFile = new File(summary.getMemoryResourceUri());
        return new SerializingCollection<V>(name, FileChannelRegistry.newFile(memoryFile), true) {

            @Override
            protected ISerde<V> newSerde() {
                return new FromBufferDelegateSerde<V>(valueSerde);
            }

            @Override
            protected InputStream newFileInputStream(final IFileChannel file) throws IOException {
                if (TimeSeriesProperties.FILE_BUFFER_CACHE_MMAP_ENABLED) {
                    readLock.lock();
                    final IMemoryMappedFile mmapFile = FileBufferCache.getFile(hashKey, summary.getMemoryResourceUri(),
                            true);
                    final boolean refCounted;
                    synchronized (mmapFile.getRefCountLock()) {
                        refCounted = mmapFile.incrementRefCount();
                    }
                    if (refCounted) {
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
                return valueFixedLength;
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
            final ArrayList<? extends MemoryFileSummary> list = getAllRangeKeys(DisabledLock.INSTANCE).getList();
            if (list.isEmpty()) {
                cachedFirstValueCopy = Optional.empty();
            } else {
                final MemoryFileSummary latestValue = list.get(0);
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
            final ArrayList<? extends MemoryFileSummary> list = getAllRangeKeys(DisabledLock.INSTANCE).getList();
            if (list.isEmpty()) {
                cachedLastValueCopy = Optional.empty();
            } else {
                final MemoryFileSummary latestValue = list.get(list.size() - 1);
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
        directoryHashKey.getDirectoryHashKeyVersion().incrementVersion();
        storage.deleteRange_latestValueLookupTable(hashKey);
        storage.deleteRange_nextValueLookupTable(hashKey);
        storage.deleteRange_previousValueLookupTable(hashKey);
        clearCaches();
    }

    public void clearCaches() {
        FileBufferCache.remove(hashKey);
        cachedAllRangeKeys.set(null);
        cachedFirstValue = null;
        cachedLastValue = null;
        cachedSize = -1L;
        latestValueIndexLookupCache.clear();
        nextValueIndexLookupCache.clear();
        previousValueIndexLookupCache.clear();
        memoryFileLookupTable.clear();
        lastResetIndex.incrementAndGet();
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
        final MemoryFileSummary summary = getLatestRangeKey(date);
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
            return summary.getPrecedingValueCount() + latestValueIndex;
        }
    }

    public V getLatestValue(final long index) {
        if (index >= size() - 1) {
            return getLastValue();
        }
        if (index <= 0) {
            return getFirstValue();
        }
        final MemoryFileSummary summary = getLatestRangeKeyIndex(index);
        if (summary == null) {
            return null;
        }
        try (IFileBufferCacheResult<V> result = getResultCached("latestValueLookupCache.loadValue", summary,
                DisabledLock.INSTANCE)) {
            final long rowIndex = index - summary.getPrecedingValueCount();
            final V latestValue = result.getLatestValue(Integers.checkedCast(rowIndex));
            if (latestValue == null) {
                return getFirstValue();
            }
            return latestValue;
        }
    }

    private V getLatestValueByValue(final FDate date) {
        final int version = directoryHashKey.getDirectoryHashKeyVersion().getVersion();
        final SingleValue value = storage.getOrLoad_latestValueLookupTable(hashKey, version, date, () -> {
            final MemoryFileSummary summary = getLatestRangeKey(date);
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
            final ArrayList<? extends MemoryFileSummary> list = getAllRangeKeys(DisabledLock.INSTANCE).getList();
            if (list.isEmpty()) {
                cachedSizeCopy = 0;
            } else {
                final MemoryFileSummary lastValue = list.get(list.size() - 1);
                cachedSizeCopy = lastValue.getCombinedValueCount();
            }
            cachedSize = cachedSizeCopy;
        }
        return cachedSizeCopy;
    }

    public long size(final FDate from, final FDate to) {
        switch (lookupMode) {
        case Value:
            return sizeByValue(from, to);
        case ValueUntilIndexAvailable:
        case Index:
            return sizeByIndex(from, to);
        default:
            throw UnknownArgumentException.newInstance(TimeSeriesLookupMode.class, lookupMode);
        }
    }

    private long sizeByValue(final FDate from, final FDate to) {
        long size = 0L;
        try (ICloseableIterator<MemoryFileSummary> fileIterator = readRangeFiles(from, to, DisabledLock.INSTANCE, null)
                .iterator()) {
            boolean first = true;
            while (true) {
                final MemoryFileSummary summary = fileIterator.next();
                if (first) {
                    first = false;
                    try (IFileBufferCacheResult<V> serializingCollection = getResultCached(READ_RANGE_VALUES, summary,
                            DisabledLock.INSTANCE)) {
                        size += serializingCollection.size(extractEndTime, from, to);
                    }
                } else if (fileIterator.hasNext()) {
                    size += summary.getValueCount();
                } else {
                    try (IFileBufferCacheResult<V> serializingCollection = getResultCached(READ_RANGE_VALUES, summary,
                            DisabledLock.INSTANCE)) {
                        size += serializingCollection.size(extractEndTime, from, to);
                    }
                }
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
        return size;
    }

    private long sizeByIndex(final FDate from, final FDate to) {
        final long toIndex = getLatestValueIndex(to);
        if (toIndex < 0) {
            return 0L;
        }
        long fromIndex = Longs.max(0, getLatestValueIndex(from));
        final V fromValue = getLatestValue(fromIndex);
        final FDate fromValueKey = extractEndTime(fromValue);
        if (fromValueKey.isBeforeNotNullSafe(from)) {
            fromIndex++;
        }
        return toIndex - fromIndex + 1;
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
        final FDate firstTime = extractEndTime(firstValue);
        if (date.isBeforeOrEqualToNotNullSafe(firstTime)) {
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
                return TimeSeriesLookupStorageCache.this.getLatestValue(index);
            }

            @Override
            protected long getLatestValueIndex(final FDate date) {
                return TimeSeriesLookupStorageCache.this.getLatestValueIndex(date);
            }

            @Override
            protected FDate extractEndTime(final V value) {
                return TimeSeriesLookupStorageCache.this.extractEndTime(value);
            }

            @Override
            protected long size() {
                return TimeSeriesLookupStorageCache.this.size();
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
        final FDate firstTime = extractEndTime(firstValue);
        if (date.isBeforeOrEqualToNotNullSafe(firstTime)) {
            return firstValue;
        } else {
            final int version = directoryHashKey.getDirectoryHashKeyVersion().getVersion();
            final SingleValue value = storage.getOrLoad_previousValueLookupTable(hashKey, version, date, shiftBackUnits,
                    () -> {
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
        final FDate lastTime = extractEndTime(lastValue);
        if (date.isAfterOrEqualToNotNullSafe(lastTime)) {
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
                return TimeSeriesLookupStorageCache.this.getLatestValue(index);
            }

            @Override
            protected long getLatestValueIndex(final FDate date) {
                return TimeSeriesLookupStorageCache.this.getLatestValueIndex(date);
            }

            @Override
            protected FDate extractEndTime(final V value) {
                return TimeSeriesLookupStorageCache.this.extractEndTime(value);
            }

            @Override
            protected long size() {
                return TimeSeriesLookupStorageCache.this.size();
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
        final FDate lastTime = extractEndTime(lastValue);
        if (date.isAfterOrEqualToNotNullSafe(lastTime)) {
            return lastValue;
        } else {
            final int version = directoryHashKey.getDirectoryHashKeyVersion().getVersion();
            final SingleValue value = storage.getOrLoad_nextValueLookupTable(hashKey, version, date, shiftForwardUnits,
                    () -> {
                        final ShiftForwardUnitsLoop<V> shiftForwardLoop = new ShiftForwardUnitsLoop<>(date,
                                shiftForwardUnits, extractEndTime);
                        final ICloseableIterator<V> rangeValues = readRangeValues(date, null, DisabledLock.INSTANCE,
                                new ISkipMemoryFileSummaryFunction() {
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
        final MemoryFileMetadata metadata = memoryFileLookupTable.getMetadata();
        final long expectedMemoryFileSize;
        try (ICloseableProperties properties = metadata.getProperties()) {
            expectedMemoryFileSize = metadata.getExpectedMemoryFileSize(properties);
        }
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

    public void assertSummary(final MemoryFileSummary prevSummary, final MemoryFileSummary summary) {
        final V firstValue = summary.getFirstValue(valueSerde);
        final FDate firstValueTime = extractEndTime(firstValue);
        if (prevSummary != null) {
            final V precedingLastValue = prevSummary.getLastValue(valueSerde);
            final FDate precedingLastValueTime = extractEndTime(precedingLastValue);
            if (precedingLastValueTime.isAfterNotNullSafe(firstValueTime)) {
                throw new IllegalStateException("precedingLastValueTime [" + precedingLastValueTime
                        + "] should not be after firstValueTime [" + firstValueTime + "]");
            }
            final long memoryOffset = summary.getPrecedingMemoryOffset() + summary.getMemoryOffset();
            final long expectedMemoryOffset = prevSummary.getPrecedingMemoryOffset() + prevSummary.getMemoryOffset()
                    + prevSummary.getMemoryLength();
            if (memoryOffset != expectedMemoryOffset) {
                throw new IllegalStateException(
                        "memoryOffset[" + memoryOffset + "] != expectedMemoryOffset[" + expectedMemoryOffset + "]");
            }

            final File memoryFile = new File(summary.getMemoryResourceUri());
            final long memoryFileSize = summary.getPrecedingMemoryOffset() + memoryFile.length();
            final long expectedMemoryFileSize = summary.getPrecedingMemoryOffset() + summary.getMemoryOffset()
                    + summary.getMemoryLength();
            if (memoryFileSize != expectedMemoryFileSize) {
                throw new IllegalStateException("memoryFileSize[" + memoryFileSize + "] != expectedMemoryFileSize["
                        + expectedMemoryFileSize + "]");
            }
        }
        final V lastValue = summary.getLastValue(valueSerde);
        final FDate lastValueTime = extractEndTime(lastValue);
        if (firstValueTime.isAfterNotNullSafe(lastValueTime)) {
            throw new IllegalStateException("firstValueTime [" + firstValueTime
                    + "] should not be after lastValueTime [" + lastValueTime + "]");
        }
    }

    /**
     * When shouldRedoLastFile=true this deletes the last file in order to create a new updated one (so the files do not
     * get fragmented too much between updates
     */
    public synchronized TimeSeriesUpdateTransaction<V> newUpdateTransaction(final boolean shouldRedoLastFile) {
        final MemoryFileSummary latestSummary = getLastRangeKey();
        final FDate updateFrom;
        final List<V> lastValues;
        final long precedingMemoryOffset;
        final long memoryOffset;
        final long precedingValueCount;
        if (latestSummary != null) {
            final FDate latestRangeKey;
            if (shouldRedoLastFile && MemoryFiles.isIncompleteMemoryFile(latestSummary.getMemoryResourceUri())) {
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
                    updateFrom = extractEndTime(lastValue);
                    latestRangeKey = latestSummary.getFirstValueEndTime();
                } else {
                    precedingMemoryOffset = latestSummary.getPrecedingMemoryOffset();
                    memoryOffset = latestSummary.getMemoryOffset() + latestSummary.getMemoryLength() + 1L;
                    precedingValueCount = latestSummary.getPrecedingValueCount() + latestSummary.getValueCount();
                    updateFrom = latestSummary.getFirstValueEndTime();
                    latestRangeKey = latestSummary.getFirstValueEndTime().addPicoseconds(1);
                }
            } else {
                lastValues = Collections.emptyList();
                precedingMemoryOffset = latestSummary.getPrecedingMemoryOffset();
                memoryOffset = latestSummary.getMemoryOffset() + latestSummary.getMemoryLength() + 1L;
                precedingValueCount = latestSummary.getPrecedingValueCount() + latestSummary.getValueCount();
                updateFrom = latestSummary.getFirstValueEndTime();
                latestRangeKey = latestSummary.getFirstValueEndTime().addPicoseconds(1);
            }
            storage.deleteRange_latestValueLookupTable(hashKey, latestRangeKey);
            storage.deleteRange_nextValueLookupTable(hashKey); //we cannot be sure here about the date since shift keys can be arbitrarily large
            storage.deleteRange_previousValueLookupTable(hashKey, latestRangeKey);
            latestValueIndexLookupCache.clear();
            nextValueIndexLookupCache.clear();
            previousValueIndexLookupCache.clear();
            lastResetIndex.incrementAndGet();
        } else {
            updateFrom = null;
            lastValues = Collections.emptyList();
            precedingMemoryOffset = 0L;
            memoryOffset = 0L;
            precedingValueCount = 0L;
        }
        clearCaches();
        return new TimeSeriesUpdateTransaction<>(this, updateFrom, lastValues, precedingMemoryOffset, memoryOffset,
                precedingValueCount);
    }

    private void assertShiftUnitsPositiveNonZero(final int shiftUnits) {
        if (shiftUnits < 0) {
            throw new IllegalArgumentException("shiftUnits needs to be a positive or zero value: " + shiftUnits);
        }
    }

    private ArrayFileBufferCacheResult<MemoryFileSummary> getAllRangeKeys(final ILock readLock) {
        ArrayFileBufferCacheResult<MemoryFileSummary> cachedAllRangeKeysCopy = cachedAllRangeKeys.get();
        if (cachedAllRangeKeysCopy == null) {
            readLock.lock();
            try {
                cachedAllRangeKeysCopy = cachedAllRangeKeys.get();
                if (cachedAllRangeKeysCopy == null) {
                    try (ICloseableIterator<MemoryFileSummary> range = memoryFileLookupTable.range()) {
                        final ArrayList<MemoryFileSummary> allRangeKeys = new ArrayList<>();
                        Lists.toListWithoutHasNext(range, allRangeKeys);
                        cachedAllRangeKeysCopy = new ArrayFileBufferCacheResult<MemoryFileSummary>(allRangeKeys);
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
            return TimeSeriesLookupStorageCache.this.getLatestValueIndex(key);
        }

        @Override
        protected V getLatestValue(final long index) {
            return TimeSeriesLookupStorageCache.this.getLatestValue(index);
        }

        @Override
        protected FDate extractEndTime(final V value) {
            return TimeSeriesLookupStorageCache.this.extractEndTime(value);
        }

        @Override
        protected int getLastResetIndex() {
            return lastResetIndex.get();
        }
    }

    private static final class MmapInputStream extends SimpleDelegateInputStream {
        private IMemoryMappedFile mmapFile;
        private Lock lock;

        private MmapInputStream(final Lock lock, final InputStream delegate, final IMemoryMappedFile mmapFile) {
            super(delegate);
            this.lock = lock;
            this.mmapFile = mmapFile;
        }

        /**
         * pattern similar to PreLockedDelegateInputStream
         */
        @Override
        public void close() throws IOException {
            if (lock != null) {
                synchronized (this) {
                    final Lock lockCopy = lock;
                    if (lockCopy != null) {
                        final IMemoryMappedFile mmapFileCopy = mmapFile;
                        if (mmapFileCopy != null) {
                            super.close();
                            synchronized (mmapFileCopy.getRefCountLock()) {
                                mmapFileCopy.decrementRefCount();
                            }
                            mmapFile = null;
                        }
                        lockCopy.unlock();
                        lock = null;
                    }
                }
            }
        }
    }

    @SuppressWarnings("rawtypes")
    private static final class ResultCacheKey {

        private final MemoryFileSummary summary;
        private final IFileBufferSource source;
        private final int hashCode;

        private ResultCacheKey(final MemoryFileSummary summary, final IFileBufferSource source) {
            this.summary = summary;
            this.source = source;
            this.hashCode = Objects.hashCode(ResultCacheKey.class, summary);
        }

        public MemoryFileSummary getSummary() {
            return summary;
        }

        public IFileBufferSource getSource() {
            return source;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj instanceof ResultCacheKey) {
                final ResultCacheKey cObj = (ResultCacheKey) obj;
                return hashCode == cObj.hashCode;
            }
            return false;
        }

    }

}
