package de.invesdwin.context.persistence.timeseriesdb.segmented.live;

public interface ILiveSegmentedTimeSeriesDBInternals<K, V> extends ILiveSegmentedTimeSeriesDB<K, V> {

    ALiveSegmentedTimeSeriesStorageCache<K, V> getLiveSegmentedLookupTableCache(K key);

}
