package de.invesdwin.context.persistence.timeseries.ipc.message;

import javax.annotation.concurrent.Immutable;

@Immutable
public class EmptySynchronousMessage<M> implements ISynchronousMessage<M> {

    public static final int TYPE = -1;
    public static final int SEQUENCE = -1;

    @SuppressWarnings("rawtypes")
    private static final EmptySynchronousMessage INSTANCE = new EmptySynchronousMessage<>();

    @SuppressWarnings("unchecked")
    public static <T> EmptySynchronousMessage<T> getInstance() {
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
