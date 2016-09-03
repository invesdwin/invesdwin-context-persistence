package de.invesdwin.context.persistence.leveldb.ipc.queue;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.SynchronousQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.leveldb.ipc.ISynchronousWriter;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.bean.tuple.Pair;

@NotThreadSafe
public class QueueSynchronousWriter implements ISynchronousWriter {

    public static final Pair<Integer, byte[]> CLOSED_MESSAGE = Pair.of(null, null);
    private Queue<Pair<Integer, byte[]>> queue;

    public QueueSynchronousWriter(final Queue<Pair<Integer, byte[]>> queue) {
        Assertions.assertThat(queue)
                .as("this implementation does not support non-blocking calls")
                .isNotInstanceOf(SynchronousQueue.class);
        this.queue = queue;
    }

    @Override
    public void open() throws IOException {}

    @Override
    public void close() throws IOException {
        queue.add(CLOSED_MESSAGE);
        queue = null;
    }

    @Override
    public void write(final int type, final byte[] message) throws IOException {
        synchronized (queue) {
            queue.add(Pair.of(type, message));
        }
    }

}
