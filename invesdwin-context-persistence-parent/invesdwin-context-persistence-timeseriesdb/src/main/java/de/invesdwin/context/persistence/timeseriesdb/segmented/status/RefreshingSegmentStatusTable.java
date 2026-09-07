package de.invesdwin.context.persistence.timeseriesdb.segmented.status;

import java.util.Map.Entry;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.directory.version.hashkey.version.ITimeSeriesDirectoryHashKeyVersion;
import de.invesdwin.context.persistence.timeseriesdb.directory.version.hashkey.version.data.ITimeSeriesDirectoryHashKeyVersionData;
import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentStatus;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.time.range.TimeRange;

@ThreadSafe
public class RefreshingSegmentStatusTable implements ISegmentStatusTable {

    private final ITimeSeriesDirectoryHashKeyVersionData directoryHashKeyVersionSegmentStatus;
    private final ITimeSeriesDirectoryHashKeyVersion directoryVersion;
    private VersionedSegmentStatusTable delegate;

    public RefreshingSegmentStatusTable(
            final ITimeSeriesDirectoryHashKeyVersionData directoryVersionHashKeySegmentStatus) {
        this.directoryHashKeyVersionSegmentStatus = directoryVersionHashKeySegmentStatus;
        this.directoryVersion = directoryVersionHashKeySegmentStatus.getParent();
    }

    private ISegmentStatusTable getDelegate() {
        if (delegate == null || delegate.getVersion() != directoryVersion.getVersion()) {
            synchronized (this) {
                if (delegate == null || delegate.getVersion() != directoryVersion.getVersion()) {
                    delegate = new VersionedSegmentStatusTable(
                            directoryHashKeyVersionSegmentStatus.getDirectoryHashKeyVersionDataShared(),
                            directoryVersion.getVersion());
                }
            }
        }
        return delegate;
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

}
