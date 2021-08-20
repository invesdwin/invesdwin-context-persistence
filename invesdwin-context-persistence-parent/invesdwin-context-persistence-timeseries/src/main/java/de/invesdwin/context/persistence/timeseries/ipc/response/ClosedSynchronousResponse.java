package de.invesdwin.context.persistence.timeseries.ipc.response;

import javax.annotation.concurrent.Immutable;

@Immutable
public class ClosedSynchronousResponse<M> implements ISynchronousResponse<M> {

    @SuppressWarnings("rawtypes")
    private static final ClosedSynchronousResponse INSTANCE = new ClosedSynchronousResponse<>();

    @SuppressWarnings("unchecked")
    public static <T> ClosedSynchronousResponse<T> getInstance() {
        return INSTANCE;
    }

    @Override
    public int getType() {
        return -1;
    }

    @Override
    public int getSequence() {
        return -1;
    }

    @Override
    public M getMessage() {
        return null;
    }

}
