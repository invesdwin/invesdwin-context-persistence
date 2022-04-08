package de.invesdwin.context.persistence.timeseriesdb.updater.progress;

import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesStorageCache;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.time.date.FDate;

public interface ITimeSeriesUpdaterInternalMethods<K, V> {

    ISerde<V> getValueSerde();

    TimeSeriesStorageCache<K, V> getLookupTable();

    ATimeSeriesDB<K, V> getTable();

    FDate extractEndTime(V element);

    void onFlush(int flushIndex, IUpdateProgress<K, V> updateProgress);

    K getKey();

}
