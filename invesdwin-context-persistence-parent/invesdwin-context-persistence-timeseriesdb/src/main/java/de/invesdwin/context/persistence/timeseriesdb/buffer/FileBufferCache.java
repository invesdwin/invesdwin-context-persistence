package de.invesdwin.context.persistence.timeseriesdb.buffer;

import java.io.File;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.ThreadSafe;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;

import de.invesdwin.context.beans.hook.ReinitializationHookManager;
import de.invesdwin.context.beans.hook.ReinitializationHookSupport;
import de.invesdwin.context.persistence.timeseriesdb.IDeserializingCloseableIterable;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.context.persistence.timeseriesdb.buffer.source.IFileBufferSource;
import de.invesdwin.context.persistence.timeseriesdb.storage.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.pool.AgronaObjectPool;
import de.invesdwin.util.concurrent.pool.IObjectPool;
import de.invesdwin.util.concurrent.pool.MemoryLimit;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.file.IMemoryMappedFile;
import de.invesdwin.util.time.date.FTimeUnit;

@SuppressWarnings({ "unchecked", "rawtypes" })
@ThreadSafe
public final class FileBufferCache {

    private static final WrappedExecutorService PRELOAD_EXECUTOR;
    private static final WrappedExecutorService LOAD_EXECUTOR;

    static {
        if (TimeSeriesProperties.FILE_BUFFER_CACHE_SEGMENTS_ENABLED
                && TimeSeriesProperties.FILE_BUFFER_CACHE_PRELOAD_ENABLED) {
            PRELOAD_EXECUTOR = Executors.newFixedThreadPool(FileBufferCache.class.getSimpleName() + "_PRELOAD", 1);
        } else {
            PRELOAD_EXECUTOR = null;
        }
        LOAD_EXECUTOR = Executors.newFixedThreadPool(FileBufferCache.class.getSimpleName() + "_LOAD", 1);
    }

    private static final AsyncLoadingCache<ResultCacheKey, SoftReference<IFileBufferCacheResult>> RESULT_CACHE;
    private static final LoadingCache<FileCacheKey, IMemoryMappedFile> FILE_CACHE;

    private static final IObjectPool<ArrayList> LIST_POOL = new AgronaObjectPool<ArrayList>(
            () -> new ArrayList<>(ATimeSeriesUpdater.DEFAULT_BATCH_FLUSH_INTERVAL),
            TimeSeriesProperties.FILE_BUFFER_CACHE_MAX_SEGMENTS_COUNT);

    /**
     * Only allow one thread at a time to clear the cache.
     */
    private static final ILock RESULT_CACHE_CLEAR_LOCK = ILockCollectionFactory.getInstance(true)
            .newLock(FileBufferCache.class.getSimpleName() + "_RESULT_CACHE_CLEAR_LOCK");

    static {
        RESULT_CACHE = Caffeine.newBuilder()
                .maximumSize(TimeSeriesProperties.FILE_BUFFER_CACHE_MAX_SEGMENTS_COUNT)
                .expireAfterAccess(
                        TimeSeriesProperties.FILE_BUFFER_CACHE_EVICTION_TIMEOUT.longValue(FTimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .removalListener(FileBufferCache::resultCache_onRemoval)
                .executor(LOAD_EXECUTOR)
                .<ResultCacheKey, SoftReference<IFileBufferCacheResult>> buildAsync(FileBufferCache::resultCache_load);
        FILE_CACHE = Caffeine.newBuilder()
                .maximumSize(TimeSeriesProperties.FILE_BUFFER_CACHE_MAX_MMAP_COUNT)
                .expireAfterAccess(
                        TimeSeriesProperties.FILE_BUFFER_CACHE_EVICTION_TIMEOUT.longValue(FTimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .removalListener(FileBufferCache::fileCache_onRemoval)
                .<FileCacheKey, IMemoryMappedFile> build(FileBufferCache::fileCache_load);
        ReinitializationHookManager.register(new ReinitializationHookSupport() {
            @Override
            public void reinitializationStarted() {
                RESULT_CACHE.asMap().clear();
                FILE_CACHE.asMap().clear();
            }
        });
    }

    private FileBufferCache() {}

    private static void resultCache_onRemoval(final ResultCacheKey key,
            final SoftReference<IFileBufferCacheResult> valueHolder, final RemovalCause cause) {
        if (valueHolder == null) {
            return;
        }
        final IFileBufferCacheResult value = valueHolder.get();
        if (value instanceof ArrayFileBufferCacheResult) {
            final ArrayFileBufferCacheResult cValue = (ArrayFileBufferCacheResult) value;
            if (cValue.isUsed() && cValue.getRefCount().get() == 0) {
                final ArrayList arrayList = cValue.getList();
                arrayList.clear();
                LIST_POOL.returnObject(arrayList);
            }
        }
    }

    private static SoftReference resultCache_load(final ResultCacheKey key) throws Exception {
        final IFileBufferSource source = key.getSource();
        final ILock readLock = source.getReadLock();
        if (!readLock.tryLock()) {
            //prevent async deadlock when write lock is active
            key.setSource(null);
            return null;
        }
        try {
            final IByteBuffer buffer = source.getBuffer();
            if (buffer != null) {
                return new SoftReference<IFileBufferCacheResult>(
                        new ByteBufferFileBufferCacheResult<>(buffer, source.getSerde(), source.getFixedLength()));
            } else {
                final IDeserializingCloseableIterable iterable = source.getIterable();
                if (TimeSeriesProperties.FILE_BUFFER_CACHE_FLYWEIGHT_ARRAY_ALLOCATOR != null
                        && iterable.getFixedLength() != null && iterable.getFixedLength() > 0) {
                    return new SoftReference<IFileBufferCacheResult>(new ByteBufferFileBufferCacheResult<>(
                            TimeSeriesProperties.FILE_BUFFER_CACHE_FLYWEIGHT_ARRAY_ALLOCATOR, iterable));
                } else {
                    key.setSource(null);
                    final ArrayList list = LIST_POOL.borrowObject();
                    try (ICloseableIterator it = iterable.iterator()) {
                        while (true) {
                            list.add(it.next());
                        }
                    } catch (final NoSuchElementException e) {
                        //end reached
                    }
                    return new SoftReference<IFileBufferCacheResult>(new ArrayFileBufferCacheResult(list));
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    private static void fileCache_onRemoval(final FileCacheKey key, final IMemoryMappedFile value,
            final RemovalCause cause) {
        if (value.getRefCount() == 0) {
            /*
             * close directly if possible if not in use
             * 
             * otherwise let the garbage collector and finalizer handle it later
             */
            value.close();
        }
    }

    private static IMemoryMappedFile fileCache_load(final FileCacheKey key) {
        final File memoryFile = key.getMemoryFile();
        try {
            return IMemoryMappedFile.map(memoryFile, 0L, memoryFile.length(), true, key.isCloseAllowed());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void remove(final String hashKey) {
        resultCache_remove(hashKey);
        fileCache_remove(hashKey);
    }

    private static void resultCache_remove(final String hashKey) {
        final Set<Entry<ResultCacheKey, CompletableFuture<SoftReference<IFileBufferCacheResult>>>> entries = RESULT_CACHE
                .asMap()
                .entrySet();
        final Iterator<Entry<ResultCacheKey, CompletableFuture<SoftReference<IFileBufferCacheResult>>>> iterator = entries
                .iterator();
        try {
            while (true) {
                final Entry<ResultCacheKey, CompletableFuture<SoftReference<IFileBufferCacheResult>>> next = iterator
                        .next();
                if (next.getKey().getHashKey().equals(hashKey)) {
                    iterator.remove();
                }
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
    }

    private static void fileCache_remove(final String hashKey) {
        final Set<Entry<FileCacheKey, IMemoryMappedFile>> entries = FILE_CACHE.asMap().entrySet();
        final Iterator<Entry<FileCacheKey, IMemoryMappedFile>> iterator = entries.iterator();
        try {
            while (true) {
                final Entry<FileCacheKey, IMemoryMappedFile> next = iterator.next();
                if (next.getKey().getHashKey().equals(hashKey)) {
                    iterator.remove();
                }
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
    }

    public static <T> IFileBufferCacheResult<T> getResult(final String hashKey, final MemoryFileSummary summary,
            final IFileBufferSource source) {
        if (TimeSeriesProperties.FILE_BUFFER_CACHE_SEGMENTS_ENABLED) {
            if (source.getReadLock().isLocked()) {
                //prevent async deadlock when write lock is active
                return getResultNoCache(source);
            }
            final ResultCacheKey key = new ResultCacheKey(hashKey, summary, source);
            final ConcurrentMap<ResultCacheKey, ?> asMap = RESULT_CACHE.asMap();
            if (MemoryLimit.isMemoryLimitReached() && !asMap.containsKey(key)) {
                if (!MemoryLimit.maybeClearCacheUnchecked(FileBufferCache.class, "RESULT_CACHE", RESULT_CACHE,
                        RESULT_CACHE_CLEAR_LOCK, TimeSeriesProperties.FILE_BUFFER_CACHE_MIN_SEGMENTS_COUNT)) {
                    maybeEvictOneResult(asMap);
                }
            }
            try {
                final SoftReference<IFileBufferCacheResult> valueHolder = Futures.getNoInterrupt(RESULT_CACHE.get(key),
                        TimeSeriesProperties.FILE_BUFFER_CACHE_ASYNC_TIMEOUT);
                if (valueHolder == null) {
                    RESULT_CACHE.asMap().remove(key);
                    return getResultNoCache(source);
                }
                final IFileBufferCacheResult value = valueHolder.get();
                if (value == null) {
                    RESULT_CACHE.asMap().remove(key);
                    return getResultNoCache(source);
                }
                return value;
            } catch (final TimeoutException e) {
                //prevent async deadlock when write lock just became active
                return getResultNoCache(source);
            }
        } else {
            return getResultNoCache(source);
        }
    }

    protected static void maybeEvictOneResult(final ConcurrentMap<ResultCacheKey, ?> asMap) {
        if (asMap.size() >= TimeSeriesProperties.FILE_BUFFER_CACHE_MIN_SEGMENTS_COUNT) {
            synchronized (RESULT_CACHE) {
                if (asMap.size() >= TimeSeriesProperties.FILE_BUFFER_CACHE_MIN_SEGMENTS_COUNT) {
                    /*
                     * either clear the whole cache in maybeClearCacheUnchecked or evict one element to make space for
                     * loading another element; don't know which order the iterator will go through the elements
                     * (hopefully least recently accessed/added), maybe picking a random one or using a heuristic based
                     * on time comparisons would be better (e.g. when this request is earlier, evict the newest and vice
                     * versa)
                     */
                    final Iterator<ResultCacheKey> iterator = asMap.keySet().iterator();
                    while (iterator.hasNext()) {
                        final ResultCacheKey first = iterator.next();
                        if (asMap.remove(first) != null) {
                            break;
                        }
                    }
                }
            }
        }
    }

    private static <T> IFileBufferCacheResult<T> getResultNoCache(final IFileBufferSource source) {
        final IByteBuffer buffer = source.getBuffer();
        if (buffer != null) {
            return new ByteBufferFileBufferCacheResult<>(buffer, source.getSerde(), source.getFixedLength());
        } else {
            return new IterableFileBufferCacheResult(source.getIterable());
        }
    }

    public static IMemoryMappedFile getFile(final String hashKey, final String memoryFilePath,
            final boolean closeAllowed) {
        if (TimeSeriesProperties.FILE_BUFFER_CACHE_MMAP_ENABLED) {
            final FileCacheKey key = new FileCacheKey(hashKey, new File(memoryFilePath), closeAllowed);
            final IMemoryMappedFile value = FILE_CACHE.get(key);
            return value;
        } else {
            return null;
        }
    }

    public static <T> void preloadResult(final String hashKey, final MemoryFileSummary summary,
            final IFileBufferSource source) {
        if (PRELOAD_EXECUTOR == null) {
            return;
        }
        if (PRELOAD_EXECUTOR.getPendingCount() > 3) {
            return;
        }
        if (source.getReadLock().isLocked()) {
            //prevent async deadlock when write lock is active
            return;
        }
        PRELOAD_EXECUTOR.execute(() -> getResult(hashKey, summary, source));
    }

    private static final class ResultCacheKey {

        private final String hashKey;
        private final MemoryFileSummary summary;
        private IFileBufferSource source;
        private final int hashCode;

        private ResultCacheKey(final String hashKey, final MemoryFileSummary summary, final IFileBufferSource source) {
            this.hashKey = hashKey;
            this.summary = summary;
            this.source = source;
            this.hashCode = Objects.hashCode(ResultCacheKey.class, hashKey, summary);
        }

        public String getHashKey() {
            return hashKey;
        }

        public MemoryFileSummary getSummary() {
            return summary;
        }

        public IFileBufferSource getSource() {
            return source;
        }

        public void setSource(final IFileBufferSource source) {
            this.source = source;
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

    private static final class FileCacheKey {

        private final String hashKey;
        private final File memoryFile;
        private final boolean closeAllowed;
        private final int hashCode;

        private FileCacheKey(final String hashKey, final File memoryFile, final boolean closeAllowed) {
            this.hashKey = hashKey;
            this.memoryFile = memoryFile;
            this.closeAllowed = closeAllowed;
            this.hashCode = Objects.hashCode(FileCacheKey.class, hashKey, memoryFile, closeAllowed);
        }

        public String getHashKey() {
            return hashKey;
        }

        public File getMemoryFile() {
            return memoryFile;
        }

        public boolean isCloseAllowed() {
            return closeAllowed;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj instanceof FileCacheKey) {
                final FileCacheKey cObj = (FileCacheKey) obj;
                return Objects.equals(hashKey, cObj.hashKey) && Objects.equals(memoryFile, cObj.memoryFile)
                        && Objects.equals(closeAllowed, cObj.closeAllowed);
            }
            return false;
        }

    }

}
