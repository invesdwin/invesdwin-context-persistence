package de.invesdwin.context.persistence.timeseriesdb.updater;

import de.invesdwin.context.integration.concurrent.nonblocking.INonBlockingRunnable;

public interface ILazyDataUpdater<K, V> {

    default INonBlockingRunnable getNonBlocking() {
        return maybeUpdateNonBlocking(false);
    }

    INonBlockingRunnable maybeUpdateNonBlocking(boolean force);

    default boolean maybeUpdate() {
        return maybeUpdate(false);
    }

    boolean maybeUpdate(boolean force);

}
