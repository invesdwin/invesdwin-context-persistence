package de.invesdwin.context.persistence.timeseriesdb.segmented.status;

import java.io.File;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.directory.version.ITimeSeriesDirectoryVersion;
import de.invesdwin.util.collections.eviction.EvictionMode;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import de.invesdwin.util.collections.loadingcache.ILoadingCache;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.streams.closeable.ISafeCloseable;

@ThreadSafe
public class SegmentStatusTable implements ISafeCloseable {

    private final ILoadingCache<String, SegmentStatusTableFolder> folderCache = new ALoadingCache<String, SegmentStatusTableFolder>() {
        @Override
        protected SegmentStatusTableFolder loadValue(final String key) {
            return new SegmentStatusTableFolder(
                    new File(Files.normalizeFilename(directory.getAbsolutePath() + "/" + key)));
        }

        @Override
        protected Integer getInitialMaximumSize() {
            return TimeSeriesStorageCache.MAXIMUM_SIZE;
        }

        @Override
        protected EvictionMode getEvictionMode() {
            return TimeSeriesStorageCache.EVICTION_MODE;
        }

        @Override
        protected boolean isHighConcurrency() {
            return TimeSeriesStorageCache.HIGH_CONCURRENCY;
        }

    };
    private final ITimeSeriesDirectoryVersion directoryVersion;
    private volatile String version;
    private volatile File directory;

    public SegmentStatusTable(final ITimeSeriesDirectoryVersion directoryVersion) {
        this.directoryVersion = directoryVersion;
        this.version = directoryVersion.getVersion();
        this.directory = newDirectory(directoryVersion.getDirectoryVersionShared());
    }

    protected File newDirectory(final File baseDirectory) {
        return new File(baseDirectory, SegmentStatusTable.class.getSimpleName());
    }

    public SegmentStatusTableFolder getFolder(final String hashKey) {
        maybeReset();
        return folderCache.get(hashKey);
    }

    private void maybeReset() {
        final String newVersion = directoryVersion.getVersion();
        if (newVersion != version) {
            synchronized (this) {
                if (newVersion != version) {
                    close();
                    directory = newDirectory(directoryVersion.getDirectoryVersionShared());
                    version = newVersion;
                }
            }
        }
    }

    @Override
    public void close() {
        folderCache.clear();
    }

}
