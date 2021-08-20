package de.invesdwin.context.persistence.timeseries.ipc;

import java.io.IOException;

import de.invesdwin.context.persistence.timeseries.ipc.response.ISynchronousResponse;

public interface ISynchronousReader<M> extends ISynchronousChannel {

    boolean hasNext() throws IOException;

    ISynchronousResponse<M> readMessage() throws IOException;

}
