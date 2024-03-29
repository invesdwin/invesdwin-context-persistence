package de.invesdwin.context.persistence.timeseriesdb.segmented;

import java.io.File;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.persistentmap.CorruptedStorageException;
import de.invesdwin.context.persistence.ezdb.RangeTablePersistenceMode;
import de.invesdwin.context.persistence.ezdb.table.range.ADelegateRangeTable;
import de.invesdwin.context.persistence.timeseriesdb.storage.TimeSeriesStorage;
import de.invesdwin.util.time.range.TimeRange;

@ThreadSafe
public class SegmentedTimeSeriesStorage extends TimeSeriesStorage {

    private final ADelegateRangeTable<String, TimeRange, SegmentStatus> segmentStatusTable;

    public SegmentedTimeSeriesStorage(final File directory, final Integer valueFixedLength,
            final ICompressionFactory compressionFactory) {
        super(directory, valueFixedLength, compressionFactory);
        segmentStatusTable = new ADelegateRangeTable<String, TimeRange, SegmentStatus>("segmentStatusTable") {
            @Override
            protected boolean allowHasNext() {
                return true;
            }

            @Override
            protected File getDirectory() {
                return directory;
            }

            @Override
            protected void onDeleteTablePreparing() {
                throw new CorruptedStorageException(getName());
            }

            @Override
            protected RangeTablePersistenceMode getPersistenceMode() {
                return RangeTablePersistenceMode.MEMORY_WRITE_THROUGH_DISK;
            }
        };
    }

    public ADelegateRangeTable<String, TimeRange, SegmentStatus> getSegmentStatusTable() {
        return segmentStatusTable;
    }

    @Override
    public void close() {
        super.close();
        segmentStatusTable.close();
    }

}
