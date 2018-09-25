package de.invesdwin.context.persistence.timeseries.ipc;

import java.io.Closeable;
import java.io.IOException;

public interface ISynchronousChannel extends Closeable {

    void open() throws IOException;

}
