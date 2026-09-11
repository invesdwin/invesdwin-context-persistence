package de.invesdwin.context.persistence.timeseriesdb.segmented.status;

import java.util.Map.Entry;

import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentStatus;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLock;
import de.invesdwin.util.time.range.TimeRange;

public interface ITimeSeriesSegmentStatusTable {

    SegmentStatus get(TimeRange timeRange);

    HeartbeatFileChannelLock newInitializationFileLock(TimeRange timeRange);

    void put(TimeRange timeRange, SegmentStatus status);

    ICloseableIterator<TimeRange> rangeKeys();

    ICloseableIterator<Entry<TimeRange, SegmentStatus>> range();

    void delete(TimeRange segment);

    Entry<TimeRange, SegmentStatus> getLatest();

    Entry<TimeRange, SegmentStatus> getLatest(TimeRange timeRange);

}
