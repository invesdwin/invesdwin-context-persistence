package de.invesdwin.context.persistence.timeseries.ipc.response;

public interface ISynchronousResponse<M> {

    int getType();

    int getSequence();

    M getMessage();

}
