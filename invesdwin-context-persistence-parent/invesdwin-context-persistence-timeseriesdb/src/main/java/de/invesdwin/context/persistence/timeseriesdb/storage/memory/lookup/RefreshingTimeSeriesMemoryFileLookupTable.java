package de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup;

import java.io.File;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.ITimeSeriesDirectoryHashKeyVersion;
import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.data.ITimeSeriesDirectoryHashKeyVersionData;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.time.date.FDate;

@ThreadSafe
public class RefreshingTimeSeriesMemoryFileLookupTable implements ITimeSeriesMemoryFileLookupTable {

    private final ITimeSeriesDirectoryHashKeyVersionData directoryHashKeyVersionSegmentStatus;
    private final ITimeSeriesDirectoryHashKeyVersion directoryVersion;
    private volatile VersionedTimeSeriesMemoryFileLookupTable delegate;

    public RefreshingTimeSeriesMemoryFileLookupTable(
            final ITimeSeriesDirectoryHashKeyVersionData directoryHashKeyVersionSegmentStatus) {
        this.directoryHashKeyVersionSegmentStatus = directoryHashKeyVersionSegmentStatus;
        this.directoryVersion = directoryHashKeyVersionSegmentStatus.getParent();
    }

    private ITimeSeriesMemoryFileLookupTable getDelegate() {
        VersionedTimeSeriesMemoryFileLookupTable delegateCopy = delegate;
        if (delegateCopy == null || delegateCopy.getVersion() != directoryVersion.getVersion()) {
            synchronized (this) {
                delegateCopy = delegate;
                if (delegateCopy == null || delegateCopy.getVersion() != directoryVersion.getVersion()) {
                    delegateCopy = new VersionedTimeSeriesMemoryFileLookupTable(
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
    public void put(final MemoryFileSummary summary) {
        getDelegate().put(summary);
    }

    @Override
    public void deleteRange() {
        getDelegate().deleteRange();
    }

    @Override
    public ICloseableIterator<MemoryFileSummary> range() {
        return getDelegate().range();
    }

    @Override
    public MemoryFileMetadata getMetadata() {
        return getDelegate().getMetadata();
    }

    @Override
    public void deleteRange(final FDate latestRangeKey) {
        getDelegate().deleteRange(latestRangeKey);
    }

}
