package de.invesdwin.context.persistence.leveldb.ipc;

import java.io.IOException;

import de.invesdwin.util.bean.tuple.Pair;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;

public interface ISynchronousReader extends ISynchronousChannel {

    boolean hasNext() throws IOException;

    boolean waitForNext(Instant waitingSince, Duration maxWait) throws InterruptedException, IOException;

    Pair<Integer, byte[]> readMessage();

}
