package de.invesdwin.context.persistence.timeseries.ipc;

import java.io.IOException;

import de.invesdwin.context.persistence.timeseries.ipc.message.ISynchronousMessage;

public interface ISynchronousWriter<M> extends ISynchronousChannel {

    void write(int type, int sequence, M message) throws IOException;

    void write(ISynchronousMessage<M> message) throws IOException;

}
