package de.invesdwin.context.persistence.timeseries.ipc;

import java.io.IOException;

public interface ISynchronousWriter extends ISynchronousChannel {

    void write(int type, byte[] message) throws IOException;

}
