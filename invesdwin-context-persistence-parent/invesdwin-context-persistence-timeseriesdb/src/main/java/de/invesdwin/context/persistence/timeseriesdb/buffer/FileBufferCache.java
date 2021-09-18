package de.invesdwin.context.persistence.timeseriesdb.buffer;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.IOUtils;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;

import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.streams.pool.PooledFastByteArrayOutputStream;

@ThreadSafe
public final class FileBufferCache {

    private static final LoadingCache<FileBufferKey, PooledFastByteArrayOutputStream> CACHE;

    static {
        CACHE = Caffeine.newBuilder()
                .maximumSize(1000)
                .softValues()
                .evictionListener(FileBufferCache::onRemoval)
                .<FileBufferKey, PooledFastByteArrayOutputStream> build(FileBufferCache::load);
    }

    private FileBufferCache() {
    }

    private static void onRemoval(final FileBufferKey key, final PooledFastByteArrayOutputStream value,
            final RemovalCause cause) {
        value.close();
    }

    private static PooledFastByteArrayOutputStream load(final FileBufferKey key) throws Exception {
        //keep file input stream open as shorty as possible to prevent too many open files error
        final File file = key.getFile();
        try (InputStream fis = key.getSource().getSource(file)) {
            final PooledFastByteArrayOutputStream bos = PooledFastByteArrayOutputStream.newInstance();
            try {
                IOUtils.copy(fis, bos.asNonClosing());
            } catch (final EOFException e) {
                //end reached
            }
            key.setSource(null);
            return bos;
        } catch (final FileNotFoundException e) {
            //maybe retry because of this in the outer iterator?
            throw new RetryLaterRuntimeException(
                    "File might have been deleted in the mean time between read locks: " + file.getAbsolutePath(), e);
        }
    }

    public static void remove(final String hashKey) {
        final Set<Entry<FileBufferKey, PooledFastByteArrayOutputStream>> entries = CACHE.asMap().entrySet();
        final Iterator<Entry<FileBufferKey, PooledFastByteArrayOutputStream>> iterator = entries.iterator();
        try {
            while (true) {
                final Entry<FileBufferKey, PooledFastByteArrayOutputStream> next = iterator.next();
                if (next.getKey().getHashKey().equals(hashKey)) {
                    iterator.remove();
                }
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
    }

    public static InputStream getInputStream(final String hashKey, final File file, final IFileBufferSource source)
            throws IOException {
        final FileBufferKey key = new FileBufferKey(hashKey, file, source);
        final PooledFastByteArrayOutputStream value = CACHE.get(key);
        return value.newInputStream();
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
