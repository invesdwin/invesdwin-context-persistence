package de.invesdwin.context.persistence.timeseriesdb.filebuffer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;

import de.invesdwin.context.beans.hook.IReinitializationHook;
import de.invesdwin.context.beans.hook.ReinitializationHookManager;
import de.invesdwin.context.persistence.timeseriesdb.TimeseriesProperties;
import de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.concurrent.pool.AgronaObjectPool;
import de.invesdwin.util.concurrent.pool.IObjectPool;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.time.date.FTimeUnit;

@SuppressWarnings({ "unchecked", "rawtypes" })
@ThreadSafe
public final class FileBufferCache {

    private static final LoadingCache<FileBufferKey, ArrayFileBufferCacheResult> CACHE;

    private static final IObjectPool<ArrayList> LIST_POOL = new AgronaObjectPool<ArrayList>(
            () -> new ArrayList<>(ATimeSeriesUpdater.BATCH_FLUSH_INTERVAL),
            TimeseriesProperties.FILE_BUFFER_CACHE_MAX_COUNT);

    static {
        CACHE = Caffeine.newBuilder()
                .maximumSize(TimeseriesProperties.FILE_BUFFER_CACHE_MAX_COUNT)
                .expireAfterAccess(
                        TimeseriesProperties.FILE_BUFFER_CACHE_EVICTION_TIMEOUT.longValue(FTimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .softValues()
                .removalListener(FileBufferCache::onRemoval)
                .<FileBufferKey, ArrayFileBufferCacheResult> build(FileBufferCache::load);
        ReinitializationHookManager.register(new IReinitializationHook() {

            @Override
            public void reinitializationStarted() {
                CACHE.asMap().clear();
            }

            @Override
            public void reinitializationFinished() {
            }

            @Override
            public void reinitializationFailed() {
            }
        });
    }

    private FileBufferCache() {
    }

    private static void onRemoval(final FileBufferKey key, final ArrayFileBufferCacheResult value,
            final RemovalCause cause) {
        if (value != null && value.isUsed() && value.getRefCount().get() == 0) {
            final ArrayList arrayList = value.getList();
            arrayList.clear();
            LIST_POOL.returnObject(arrayList);
        }
    }

    private static ArrayFileBufferCacheResult load(final FileBufferKey key) throws Exception {
        //keep file input stream open as shorty as possible to prevent too many open files error
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

    public static void remove(final String hashKey) {
        final Set<Entry<FileBufferKey, ArrayFileBufferCacheResult>> entries = CACHE.asMap().entrySet();
        final Iterator<Entry<FileBufferKey, ArrayFileBufferCacheResult>> iterator = entries.iterator();
        try {
            while (true) {
                final Entry<FileBufferKey, ArrayFileBufferCacheResult> next = iterator.next();
                if (next.getKey().getHashKey().equals(hashKey)) {
                    iterator.remove();
                }
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
    }

    public static <T> IFileBufferCacheResult<T> getIterable(final String hashKey, final File file,
            final IFileBufferSource source) {
        if (TimeseriesProperties.FILE_BUFFER_CACHE_ENABLED) {
            final FileBufferKey key = new FileBufferKey(hashKey, file, source);
            final IFileBufferCacheResult value = CACHE.get(key);
            return value;
        } else {
            try {
                return new IterableFileBufferCacheResult(source.getSource());
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final class FileBufferKey {

        private final String hashKey;
        private final File file;
        private IFileBufferSource source;
        private final int hashCode;

        private FileBufferKey(final String hashKey, final File file, final IFileBufferSource source) {
            this.hashKey = hashKey;
            this.file = file;
            this.source = source;
            this.hashCode = Objects.hashCode(hashKey, file);
        }

        public String getHashKey() {
            return hashKey;
        }

        public File getFile() {
            return file;
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
            if (obj instanceof FileBufferKey) {
                final FileBufferKey cObj = (FileBufferKey) obj;
                return Objects.equals(hashKey, cObj.hashKey) && Objects.equals(file, cObj.file);
            }
            return false;
        }

    }

}
