package de.invesdwin.context.persistence.timeseries.ipc;

import java.io.IOException;

public interface ISynchronousReader extends ISynchronousChannel {

    boolean hasNext() throws IOException;

    SynchronousResponse readMessage() throws IOException;

}
