package de.invesdwin.context.persistence.leveldb.ipc;

import java.io.Closeable;
import java.io.IOException;

public interface ISynchronousChannel extends Closeable {

    void open() throws IOException;

}
