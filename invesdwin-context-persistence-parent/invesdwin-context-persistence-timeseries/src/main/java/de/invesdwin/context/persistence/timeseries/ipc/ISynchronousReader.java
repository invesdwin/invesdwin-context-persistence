package de.invesdwin.context.persistence.timeseries.ipc;

import java.io.IOException;

import de.invesdwin.context.persistence.timeseries.ipc.message.ISynchronousMessage;

public interface ISynchronousReader<M> extends ISynchronousChannel {

    boolean hasNext() throws IOException;

    ISynchronousMessage<M> readMessage() throws IOException;

}
