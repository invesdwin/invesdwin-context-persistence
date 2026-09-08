package de.invesdwin.context.persistence.timeseriesdb.segmented.status;

import java.util.Map.Entry;

import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentStatus;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.streams.closeable.ISafeCloseable;
import de.invesdwin.util.time.range.TimeRange;

public interface ITimeSeriesSegmentStatusTable extends ISafeCloseable {

    SegmentStatus get(TimeRange timeRange);

    void put(TimeRange timeRange, SegmentStatus status);

    ICloseableIterator<TimeRange> rangeKeys();

    ICloseableIterator<Entry<TimeRange, SegmentStatus>> range();

    void delete(TimeRange segment);

    void deleteRange();

    Entry<TimeRange, SegmentStatus> getLatest();

    Entry<TimeRange, SegmentStatus> getLatest(TimeRange timeRange);

}
