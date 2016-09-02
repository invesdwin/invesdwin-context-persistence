package de.invesdwin.context.persistence.leveldb.ipc;

import java.io.IOException;

import de.invesdwin.util.bean.tuple.Pair;

public interface ISynchronousReader extends ISynchronousChannel {

    boolean hasNext() throws IOException;

    Pair<Integer, byte[]> readMessage();

}
