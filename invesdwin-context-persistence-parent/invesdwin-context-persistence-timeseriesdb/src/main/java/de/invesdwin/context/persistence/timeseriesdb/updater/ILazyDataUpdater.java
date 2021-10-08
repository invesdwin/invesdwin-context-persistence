package de.invesdwin.context.persistence.timeseriesdb.updater;

public interface ILazyDataUpdater<K, V> {

    void maybeUpdate();

}
