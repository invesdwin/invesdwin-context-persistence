package de.invesdwin.context.persistence.timeseries.ipc.response;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class SynchronousResponse<M> implements ISynchronousResponse<M> {

    private final int type;
    private final int sequence;
    private final M message;

    public SynchronousResponse(final int type, final int sequence, final M message) {
        this.type = type;
        this.sequence = sequence;
        this.message = message;
    }

    private SynchronousResponse(final ISynchronousResponse<M> response) {
        this(response.getType(), response.getSequence(), response.getMessage());
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public int getSequence() {
        return sequence;
    }

    @Override
    public M getMessage() {
        return message;
    }

    public static <T> SynchronousResponse<T> valueOf(final ISynchronousResponse<T> response) {
        if (response instanceof SynchronousResponse) {
            return (SynchronousResponse<T>) response;
        } else {
            return new SynchronousResponse<T>(response);
        }
    }

}
