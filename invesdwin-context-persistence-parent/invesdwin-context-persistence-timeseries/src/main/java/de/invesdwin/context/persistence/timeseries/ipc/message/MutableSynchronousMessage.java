package de.invesdwin.context.persistence.timeseries.ipc.message;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class MutableSynchronousMessage<M> implements ISynchronousMessage<M> {

    protected int type;
    protected int sequence;
    protected M message;

    @Override
    public int getType() {
        return type;
    }

    public void setType(final int type) {
        this.type = type;
    }

    @Override
    public int getSequence() {
        return sequence;
    }

    public void setSequence(final int sequence) {
        this.sequence = sequence;
    }

    @Override
    public M getMessage() {
        return message;
    }

    public void setMessage(final M message) {
        this.message = message;
    }

}
