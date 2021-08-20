package de.invesdwin.context.persistence.timeseries.ipc.message;

public interface ISynchronousMessage<M> {

    int getType();

    int getSequence();

    M getMessage();

}
