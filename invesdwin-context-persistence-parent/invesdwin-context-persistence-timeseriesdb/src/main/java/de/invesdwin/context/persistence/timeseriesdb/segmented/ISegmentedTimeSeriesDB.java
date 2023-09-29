package de.invesdwin.context.persistence.timeseriesdb.segmented;

import de.invesdwin.context.persistence.timeseriesdb.ITimeSeriesDB;

public interface ISegmentedTimeSeriesDB<K, V> extends ITimeSeriesDB<K, V> {

    String hashKeyToString(SegmentedKey<K> segmentedKey);

}
