package de.invesdwin.context.persistence.timeseriesdb.segmented.status;

import java.util.Map.Entry;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.ITimeSeriesDirectoryHashKeyVersion;
import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.data.ITimeSeriesDirectoryHashKeyVersionData;
import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentStatus;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.concurrent.reference.MutableSoftReference;
import de.invesdwin.util.time.range.TimeRange;

@ThreadSafe
public class RefreshingTimeSeriesSegmentStatusTable implements ITimeSeriesSegmentStatusTable {

    private final ITimeSeriesDirectoryHashKeyVersionData directoryHashKeyVersionSegmentStatus;
    private final ITimeSeriesDirectoryHashKeyVersion directoryVersion;
    private final MutableSoftReference<VersionedTimeSeriesSegmentStatusTable> delegateRef = new MutableSoftReference<VersionedTimeSeriesSegmentStatusTable>(
            null);

    public RefreshingTimeSeriesSegmentStatusTable(
            final ITimeSeriesDirectoryHashKeyVersionData directoryHashKeyVersionSegmentStatus) {
        this.directoryHashKeyVersionSegmentStatus = directoryHashKeyVersionSegmentStatus;
        this.directoryVersion = directoryHashKeyVersionSegmentStatus.getParent();
    }

    private ITimeSeriesSegmentStatusTable getDelegate() {
        VersionedTimeSeriesSegmentStatusTable delegateCopy = delegateRef.get();
        if (delegateCopy == null || delegateCopy.getVersion() != directoryVersion.getVersion()) {
            synchronized (this) {
                if (delegateCopy == null || delegateCopy.getVersion() != directoryVersion.getVersion()) {
                    delegateCopy = new VersionedTimeSeriesSegmentStatusTable(
                            directoryHashKeyVersionSegmentStatus.getDirectoryHashKeyVersionDataShared(),
                            directoryVersion.getVersion());
                    delegateRef.set(delegateCopy);
                }
            }
        }
        return delegateCopy;
    }

    @Override
    public SegmentStatus get(final TimeRange timeRange) {
        return getDelegate().get(timeRange);
    }

    @Override
    public void put(final TimeRange timeRange, final SegmentStatus status) {
        getDelegate().put(timeRange, status);
    }

    @Override
    public ICloseableIterator<Entry<TimeRange, SegmentStatus>> range() {
        return getDelegate().range();
    }

    @Override
    public ICloseableIterator<TimeRange> rangeKeys() {
        return getDelegate().rangeKeys();
    }

    @Override
    public void delete(final TimeRange segment) {
        getDelegate().delete(segment);
    }

    @Override
    public void deleteRange() {
        getDelegate().deleteRange();
    }

    @Override
    public Entry<TimeRange, SegmentStatus> getLatest() {
        return getDelegate().getLatest();
    }

    @Override
    public Entry<TimeRange, SegmentStatus> getLatest(final TimeRange timeRange) {
        return getDelegate().getLatest(timeRange);
    }

    @Override
    public void close() {
        final VersionedTimeSeriesSegmentStatusTable delegateCopy = delegateRef.get();
        if (delegateCopy != null) {
            delegateCopy.close();
            delegateRef.set(null);
        }
    }

}
