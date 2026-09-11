package de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup;

import java.io.File;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.ITimeSeriesDirectoryHashKeyVersion;
import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.data.ITimeSeriesDirectoryHashKeyVersionData;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.util.collections.iterable.ICloseableIterator;

@ThreadSafe
public class RefreshingTimeSeriesMemoryFileLookupTable<V> implements ITimeSeriesMemoryFileLookupTable {

    private final TimeSeriesLookupStorageCache<?, V> parent;
    private final ITimeSeriesDirectoryHashKeyVersionData directoryHashKeyVersionSegmentStatus;
    private final ITimeSeriesDirectoryHashKeyVersion directoryVersion;
    private volatile VersionedTimeSeriesMemoryFileLookupTable<V> delegate;

    public RefreshingTimeSeriesMemoryFileLookupTable(final TimeSeriesLookupStorageCache<?, V> parent,
            final ITimeSeriesDirectoryHashKeyVersionData directoryHashKeyVersionSegmentStatus) {
        this.parent = parent;
        this.directoryHashKeyVersionSegmentStatus = directoryHashKeyVersionSegmentStatus;
        this.directoryVersion = directoryHashKeyVersionSegmentStatus.getParent();
    }

    private ITimeSeriesMemoryFileLookupTable getDelegate() {
        VersionedTimeSeriesMemoryFileLookupTable<V> delegateCopy = delegate;
        if (delegateCopy == null || delegateCopy.getVersion() != directoryVersion.getVersion()) {
            synchronized (this) {
                delegateCopy = delegate;
                if (delegateCopy == null || delegateCopy.getVersion() != directoryVersion.getVersion()) {
                    delegateCopy = new VersionedTimeSeriesMemoryFileLookupTable<V>(parent,
                            new File(directoryHashKeyVersionSegmentStatus.getDirectoryHashKeyVersionDataShared(),
                                    AMemoryFileSummarySerializingCollection.MEMORY_INDEX_FILE_NAME),
                            directoryVersion.getVersion());
                    delegate = delegateCopy;
                }
            }
        }
        return delegateCopy;
    }

    @Override
    public void put(final List<MemoryFileSummary> summaries) {
        getDelegate().put(summaries);
    }

    @Override
    public ICloseableIterator<MemoryFileSummary> range() {
        return getDelegate().range();
    }

    @Override
    public MemoryFileMetadata getMetadata() {
        return getDelegate().getMetadata();
    }

    public void clear() {
        delegate = null;
    }

}
