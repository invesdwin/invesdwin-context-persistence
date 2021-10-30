package de.invesdwin.context.persistence.timeseriesdb.buffer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;

import de.invesdwin.context.beans.hook.ReinitializationHookManager;
import de.invesdwin.context.beans.hook.ReinitializationHookSupport;
import de.invesdwin.context.persistence.timeseriesdb.TimeseriesProperties;
import de.invesdwin.context.persistence.timeseriesdb.storage.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.pool.AgronaObjectPool;
import de.invesdwin.util.concurrent.pool.IObjectPool;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.streams.buffer.MemoryMappedFile;
import de.invesdwin.util.time.date.FTimeUnit;

@SuppressWarnings({ "unchecked", "rawtypes" })
@ThreadSafe
public final class FileBufferCache {

    private static final WrappedExecutorService PRELOAD_EXECUTOR;

    static {
        if (TimeseriesProperties.FILE_BUFFER_CACHE_SEGMENTS_ENABLED
                && TimeseriesProperties.FILE_BUFFER_CACHE_PRELOAD_ENABLED) {
            PRELOAD_EXECUTOR = Executors.newFixedThreadPool(FileBufferCache.class.getSimpleName() + "_PRELOAD", 1);
        } else {
            PRELOAD_EXECUTOR = null;
        }
    }

    private static final LoadingCache<ResultCacheKey, ArrayFileBufferCacheResult> RESULT_CACHE;
    private static final LoadingCache<FileCacheKey, MemoryMappedFile> FILE_CACHE;

    private static final IObjectPool<ArrayList> LIST_POOL = new AgronaObjectPool<ArrayList>(
            () -> new ArrayList<>(ATimeSeriesUpdater.BATCH_FLUSH_INTERVAL),
            TimeseriesProperties.FILE_BUFFER_CACHE_MAX_SEGMENTS_COUNT);

    static {
        @NonNull
        final Caffeine<Object, Object> builder = Caffeine.newBuilder()
                .maximumSize(TimeseriesProperties.FILE_BUFFER_CACHE_MAX_SEGMENTS_COUNT)
                .expireAfterAccess(
                        TimeseriesProperties.FILE_BUFFER_CACHE_EVICTION_TIMEOUT.longValue(FTimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS);
        if (TimeseriesProperties.FILE_BUFFER_CACHE_WEAK_REFERENCES) {
            builder.weakValues();
        } else {
            builder.weakValues();
        }
        RESULT_CACHE = builder.removalListener(FileBufferCache::resultCache_onRemoval)
                .<ResultCacheKey, ArrayFileBufferCacheResult> build(FileBufferCache::resultCache_load);
        FILE_CACHE = Caffeine.newBuilder()
                .maximumSize(TimeseriesProperties.FILE_BUFFER_CACHE_MAX_MMAP_COUNT)
                .expireAfterAccess(
                        TimeseriesProperties.FILE_BUFFER_CACHE_EVICTION_TIMEOUT.longValue(FTimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .removalListener(FileBufferCache::fileCache_onRemoval)
                .<FileCacheKey, MemoryMappedFile> build(FileBufferCache::fileCache_load);
        ReinitializationHookManager.register(new ReinitializationHookSupport() {
            @Override
            public void reinitializationStarted() {
                RESULT_CACHE.asMap().clear();
                FILE_CACHE.asMap().clear();
            }
        });
    }

    private FileBufferCache() {
    }

    private static void resultCache_onRemoval(final ResultCacheKey key, final ArrayFileBufferCacheResult value,
            final RemovalCause cause) {
        if (value != null && value.isUsed() && value.getRefCount().get() == 0) {
            final ArrayList arrayList = value.getList();
            arrayList.clear();
            LIST_POOL.returnObject(arrayList);
        }
    }

    private static ArrayFileBufferCacheResult resultCache_load(final ResultCacheKey key) throws Exception {
        final ICloseableIterable values = key.getSource().getSource();
        key.setSource(null);
        final ArrayList list = LIST_POOL.borrowObject();
        try (ICloseableIterator it = values.iterator()) {
            while (true) {
                list.add(it.next());
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
        return new ArrayFileBufferCacheResult(list);
    }

    private static void fileCache_onRemoval(final FileCacheKey key, final MemoryMappedFile value,
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

    private static MemoryMappedFile fileCache_load(final FileCacheKey key) {
        final File memoryFile = key.getMemoryFile();
        try {
            return new MemoryMappedFile(memoryFile.getAbsolutePath(), memoryFile.length(), true);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void remove(final String hashKey) {
        resultCache_remove(hashKey);
        fileCache_remove(hashKey);
    }

    private static void resultCache_remove(final String hashKey) {
        final Set<Entry<ResultCacheKey, ArrayFileBufferCacheResult>> entries = RESULT_CACHE.asMap().entrySet();
        final Iterator<Entry<ResultCacheKey, ArrayFileBufferCacheResult>> iterator = entries.iterator();
        try {
            while (true) {
                final Entry<ResultCacheKey, ArrayFileBufferCacheResult> next = iterator.next();
                if (next.getKey().getHashKey().equals(hashKey)) {
                    iterator.remove();
                }
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
    }

    private static void fileCache_remove(final String hashKey) {
        final Set<Entry<FileCacheKey, MemoryMappedFile>> entries = FILE_CACHE.asMap().entrySet();
        final Iterator<Entry<FileCacheKey, MemoryMappedFile>> iterator = entries.iterator();
        try {
            while (true) {
                final Entry<FileCacheKey, MemoryMappedFile> next = iterator.next();
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
        if (TimeseriesProperties.FILE_BUFFER_CACHE_SEGMENTS_ENABLED) {
            final ResultCacheKey key = new ResultCacheKey(hashKey, summary, source);
            final IFileBufferCacheResult value = RESULT_CACHE.get(key);
            return value;
        } else {
            try {
                return new IterableFileBufferCacheResult(source.getSource());
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static MemoryMappedFile getFile(final String hashKey, final String memoryFilePath) {
        if (TimeseriesProperties.FILE_BUFFER_CACHE_MMAP_ENABLED) {
            final FileCacheKey key = new FileCacheKey(hashKey, new File(memoryFilePath));
            final MemoryMappedFile value = FILE_CACHE.get(key);
            return value;
        } else {
            return null;
        }
    }

    public static <T> void preloadResult(final String hashKey, final MemoryFileSummary summary,
            final IFileBufferSource source) {
        if (PRELOAD_EXECUTOR != null) {
            if (PRELOAD_EXECUTOR.getPendingCount() <= 3) {
                PRELOAD_EXECUTOR.execute(() -> getResult(hashKey, summary, source));
            }
        }
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
            this.hashCode = Objects.hashCode(hashKey, summary);
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
                return Objects.equals(getHashKey(), cObj.getHashKey())
                        && Objects.equals(getSummary(), cObj.getSummary());
            }
            return false;
        }

    }

    private static final class FileCacheKey {

        private final String hashKey;
        private final File memoryFile;
        private final int hashCode;

        private FileCacheKey(final String hashKey, final File memoryFile) {
            this.hashKey = hashKey;
            this.memoryFile = memoryFile;
            this.hashCode = Objects.hashCode(hashKey, memoryFile);
        }

        public String getHashKey() {
            return hashKey;
        }

        public File getMemoryFile() {
            return memoryFile;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj instanceof FileCacheKey) {
                final FileCacheKey cObj = (FileCacheKey) obj;
                return Objects.equals(hashKey, cObj.hashKey) && Objects.equals(memoryFile, cObj.memoryFile);
            }
            return false;
        }

    }

}
