package de.invesdwin.context.persistence.timeseriesdb;

public interface ITimeSeriesDBInternals<K, V> extends ITimeSeriesDB<K, V> {

    TimeSeriesLookupStorageCache<K, V> getLookupTableCache(K key);

}
