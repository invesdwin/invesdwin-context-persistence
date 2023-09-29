package de.invesdwin.context.persistence.timeseriesdb.segmented;

import de.invesdwin.context.persistence.timeseriesdb.ITimeSeriesDBInternals;

public interface ISegmentedTimeSeriesDBInternals<K, V> extends ISegmentedTimeSeriesDB<K, V> {

    ASegmentedTimeSeriesStorageCache<K, V> getSegmentedLookupTableCache(K key);

    ITimeSeriesDBInternals<SegmentedKey<K>, V> getSegmentedTable();

    SegmentedTimeSeriesStorage getStorage();

}
