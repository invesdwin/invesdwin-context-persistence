package de.invesdwin.context.persistence.timeseries.ipc.response;

import javax.annotation.concurrent.Immutable;

@Immutable
public class EmptySynchronousResponse<M> implements ISynchronousResponse<M> {

    public static final int TYPE = -1;
    public static final int SEQUENCE = -1;

    @SuppressWarnings("rawtypes")
    private static final EmptySynchronousResponse INSTANCE = new EmptySynchronousResponse<>();

    @SuppressWarnings("unchecked")
    public static <T> EmptySynchronousResponse<T> getInstance() {
        return INSTANCE;
    }

    @Override
    public int getType() {
        return TYPE;
    }

    @Override
    public int getSequence() {
        return SEQUENCE;
    }

    @Override
    public M getMessage() {
        return null;
    }

}
