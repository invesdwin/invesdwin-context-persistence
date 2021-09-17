package de.invesdwin.context.persistence.timeseriesdb.updater;

public interface IDataUpdater<K, V> {

    void maybeUpdate();

}
