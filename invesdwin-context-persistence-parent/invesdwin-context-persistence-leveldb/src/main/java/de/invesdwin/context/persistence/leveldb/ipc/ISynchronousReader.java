package de.invesdwin.context.persistence.leveldb.ipc;

import de.invesdwin.util.bean.tuple.Pair;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;

public interface ISynchronousReader extends ISynchronousChannel {

    boolean hasNext();

    boolean waitForNext(Instant waitingSince, Duration maxWait) throws InterruptedException;

    Pair<Integer, byte[]> readMessage();

}
