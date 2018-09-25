package de.invesdwin.context.persistence.leveldb.timeseries.segmented;

import java.io.File;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.leveldb.ezdb.ADelegateRangeTable;
import de.invesdwin.context.persistence.leveldb.timeseries.storage.CorruptedTimeSeriesStorageException;
import de.invesdwin.context.persistence.leveldb.timeseries.storage.TimeSeriesStorage;
import de.invesdwin.util.time.range.TimeRange;

@ThreadSafe
public class SegmentedTimeSeriesStorage extends TimeSeriesStorage {

    private final ADelegateRangeTable<String, TimeRange, SegmentStatus> segmentStatusTable;

    public SegmentedTimeSeriesStorage(final File directory) {
        super(directory);
        segmentStatusTable = new ADelegateRangeTable<String, TimeRange, SegmentStatus>("segmentStatusTable") {
            @Override
            protected boolean allowPutWithoutBatch() {
                return true;
            }

            @Override
            protected boolean allowHasNext() {
                return true;
            }

            @Override
            protected File getDirectory() {
                return directory;
            }

            @Override
            protected void onDeleteTableFinished() {
                throw new CorruptedTimeSeriesStorageException(getName());
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
