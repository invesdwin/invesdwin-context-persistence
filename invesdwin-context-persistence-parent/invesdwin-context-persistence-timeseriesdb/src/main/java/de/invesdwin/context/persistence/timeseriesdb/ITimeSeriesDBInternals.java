package de.invesdwin.context.persistence.timeseriesdb;

public interface ITimeSeriesDBInternals<K, V> extends ITimeSeriesDB<K, V> {

    TimeSeriesStorageCache<K, V> getLookupTableCache(K key);

}
