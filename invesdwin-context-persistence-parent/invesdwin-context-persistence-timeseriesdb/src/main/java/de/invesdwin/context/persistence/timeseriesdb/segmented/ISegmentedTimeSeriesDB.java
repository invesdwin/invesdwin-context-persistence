package de.invesdwin.context.persistence.timeseriesdb.segmented;

import de.invesdwin.context.persistence.timeseriesdb.ITimeSeriesDB;
import de.invesdwin.util.time.date.FDate;

public interface ISegmentedTimeSeriesDB<K, V> extends ITimeSeriesDB<K, V> {

    String hashKeyToString(SegmentedKey<K> segmentedKey);

    /**
     * The coverage this store DECLARES for [from,to]: covered sub-ranges (COMPLETE), covered-without-data sub-ranges
     * (COMPLETE_EMPTY), still-initializing sub-ranges (INITIALIZING) and the undeclared gap sub-ranges that remain -
     * see {@link TimeSeriesCoverage}. Derived from the segment status table ranges, not from a row scan: callers get
     * localized holes without count-probing or bisection.
     *
     * @param from
     *            query range start, null means unlimited
     * @param to
     *            query range end, null means unlimited
     */
    TimeSeriesCoverage coverage(K key, FDate from, FDate to);

}
