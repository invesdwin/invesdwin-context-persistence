package de.invesdwin.context.persistence.timeseries.ipc;

import javax.annotation.concurrent.Immutable;

@Immutable
public class SynchronousResponse {

    private final int type;
    private final int sequence;
    private final byte[] message;

    public SynchronousResponse(final int type, final int sequence, final byte[] message) {
        this.type = type;
        this.sequence = sequence;
        this.message = message;
    }

    public int getType() {
        return type;
    }

    public int getSequence() {
        return sequence;
    }

    public byte[] getMessage() {
        return message;
    }

}
