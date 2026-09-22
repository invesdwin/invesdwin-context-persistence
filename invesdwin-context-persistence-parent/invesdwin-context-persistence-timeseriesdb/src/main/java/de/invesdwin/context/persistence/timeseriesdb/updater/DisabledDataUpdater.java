package de.invesdwin.context.persistence.timeseriesdb.updater;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.concurrent.nonblocking.DisabledNonBlockingRunnable;
import de.invesdwin.context.integration.concurrent.nonblocking.INonBlockingRunnable;

@Immutable
public class DisabledDataUpdater<K, V> implements ILazyDataUpdater<K, V> {

    @SuppressWarnings("rawtypes")
    private static final DisabledDataUpdater INSTANCE = new DisabledDataUpdater<>();

    @Override
    public INonBlockingRunnable getNonBlocking(final boolean force) {
        return DisabledNonBlockingRunnable.INSTANCE;
    }

    @Override
    public boolean maybeUpdate(final boolean force) {
        //noop
        return false;
    }

    @SuppressWarnings("unchecked")
    public static <K, V> DisabledDataUpdater<K, V> getInstance() {
        return INSTANCE;
    }

}
