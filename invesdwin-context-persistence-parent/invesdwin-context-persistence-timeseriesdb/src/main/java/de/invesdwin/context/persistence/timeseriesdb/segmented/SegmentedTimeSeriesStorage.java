package de.invesdwin.context.persistence.timeseriesdb.segmented;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.persistence.timeseriesdb.directory.version.ITimeSeriesDirectoryVersion;
import de.invesdwin.context.persistence.timeseriesdb.segmented.status.SegmentStatusTable;
import de.invesdwin.context.persistence.timeseriesdb.storage.TimeSeriesStorage;

@ThreadSafe
public class SegmentedTimeSeriesStorage extends TimeSeriesStorage {

    private final SegmentStatusTable segmentStatusTable;

    public SegmentedTimeSeriesStorage(final ITimeSeriesDirectoryVersion directoryVersion,
            final Integer valueFixedLength, final ICompressionFactory compressionFactory) {
        super(directoryVersion, valueFixedLength, compressionFactory);
        segmentStatusTable = new SegmentStatusTable(directoryVersion);
    }

    public SegmentStatusTable getSegmentStatusTable() {
        return segmentStatusTable;
    }

    @Override
    public void close() {
        super.close();
        segmentStatusTable.close();
    }

}
