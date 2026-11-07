package de.invesdwin.context.persistence.timeseriesdb.segmented.status;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentStatus;
import de.invesdwin.context.persistence.timeseriesdb.segmented.TimeSeriesCoverage;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLock;
import de.invesdwin.util.time.date.FDate;
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

    /**
     * The declared coverage for [from,to] derived from these status rows only - see {@link TimeSeriesCoverage}. Cheap:
     * it reads the status ranges, never the data behind them.
     *
     * @param from
     *            query range start, null means unlimited
     * @param to
     *            query range end, null means unlimited
     */
    default TimeSeriesCoverage coverage(final FDate from, final FDate to) {
        return coverage(from, to, null);
    }

    /**
     * As {@link #coverage(FDate, FDate)}, with extra sub-ranges added to the covered bucket although they carry no
     * status row yet - e.g. the live segment of a live store, which holds data before its segment gets
     * status-declared COMPLETE.
     *
     * @param extraCoveredRanges
     *            may be null
     */
    default TimeSeriesCoverage coverage(final FDate from, final FDate to,
            final Iterable<TimeRange> extraCoveredRanges) {
        final List<Entry<TimeRange, SegmentStatus>> rows = new ArrayList<Entry<TimeRange, SegmentStatus>>();
        try (ICloseableIterator<Entry<TimeRange, SegmentStatus>> iterator = range()) {
            while (true) {
                rows.add(iterator.next());
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
        return TimeSeriesCoverage.valueOf(from, to, rows, extraCoveredRanges);
    }

}
