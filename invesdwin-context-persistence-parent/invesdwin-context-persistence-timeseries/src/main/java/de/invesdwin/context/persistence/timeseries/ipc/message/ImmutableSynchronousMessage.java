package de.invesdwin.context.persistence.timeseries.ipc.message;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class ImmutableSynchronousMessage<M> implements ISynchronousMessage<M> {

    private final int type;
    private final int sequence;
    private final M message;

    public ImmutableSynchronousMessage(final int type, final int sequence, final M message) {
        this.type = type;
        this.sequence = sequence;
        this.message = message;
    }

    private ImmutableSynchronousMessage(final ISynchronousMessage<M> response) {
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

    public static <T> ImmutableSynchronousMessage<T> valueOf(final ISynchronousMessage<T> response) {
        if (response == null) {
            return null;
        } else if (response instanceof ImmutableSynchronousMessage) {
            return (ImmutableSynchronousMessage<T>) response;
        } else {
            return new ImmutableSynchronousMessage<T>(response);
        }
    }

}
