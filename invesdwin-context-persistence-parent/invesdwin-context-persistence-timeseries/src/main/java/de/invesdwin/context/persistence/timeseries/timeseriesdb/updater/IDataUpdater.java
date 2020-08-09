package de.invesdwin.context.persistence.timeseries.timeseriesdb.updater;

public interface IDataUpdater<K, V> {

    void maybeUpdate();

}
