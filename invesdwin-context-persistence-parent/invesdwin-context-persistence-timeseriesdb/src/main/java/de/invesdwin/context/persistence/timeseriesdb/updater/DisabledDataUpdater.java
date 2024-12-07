package de.invesdwin.context.persistence.timeseriesdb.updater;

import javax.annotation.concurrent.Immutable;

@Immutable
public class DisabledDataUpdater<K, V> implements ILazyDataUpdater<K, V> {

    @SuppressWarnings("rawtypes")
    private static final DisabledDataUpdater INSTANCE = new DisabledDataUpdater<>();

    @Override
    public void maybeUpdate(final boolean force) {
        //noop
    }

    @SuppressWarnings("unchecked")
    public static <K, V> DisabledDataUpdater<K, V> getInstance() {
        return INSTANCE;
    }

}
