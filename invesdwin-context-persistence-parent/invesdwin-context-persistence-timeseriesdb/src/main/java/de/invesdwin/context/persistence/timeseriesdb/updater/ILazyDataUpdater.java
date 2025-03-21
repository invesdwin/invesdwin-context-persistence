package de.invesdwin.context.persistence.timeseriesdb.updater;

public interface ILazyDataUpdater<K, V> {

    default void maybeUpdate() {
        maybeUpdate(false);
    }

    void maybeUpdate(boolean force);

}
