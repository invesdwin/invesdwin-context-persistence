package de.invesdwin.context.persistence.timeseriesdb.updater.progress;

import de.invesdwin.context.persistence.timeseriesdb.ITimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.time.date.FDate;

public interface ITimeSeriesUpdaterInternalMethods<K, V> {

    ISerde<V> getValueSerde();

    TimeSeriesLookupStorageCache<K, V> getLookupTable();

    ITimeSeriesDB<K, V> getTable();

    FDate extractStartTime(V element);

    FDate extractEndTime(V element);

    void onElement(ITimeSeriesUpdateProgress updateProgress, long pendingCount);

    void onFlush(ITimeSeriesUpdateProgress updateProgress, long flushIndex);

    K getKey();

    boolean shouldRedoLastFile();

}
