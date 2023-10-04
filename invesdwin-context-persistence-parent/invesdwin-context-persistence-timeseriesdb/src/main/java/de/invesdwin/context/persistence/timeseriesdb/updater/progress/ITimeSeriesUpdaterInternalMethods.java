package de.invesdwin.context.persistence.timeseriesdb.updater.progress;

import de.invesdwin.context.persistence.timeseriesdb.ITimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesStorageCache;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.time.date.FDate;

public interface ITimeSeriesUpdaterInternalMethods<K, V> {

    ISerde<V> getValueSerde();

    TimeSeriesStorageCache<K, V> getLookupTable();

    ITimeSeriesDB<K, V> getTable();

    FDate extractStartTime(V element);

    FDate extractEndTime(V element);

    void onFlush(int flushIndex, IUpdateProgress<K, V> updateProgress);

    K getKey();

}
