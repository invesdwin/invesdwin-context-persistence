package de.invesdwin.context.persistence.timeseries.ipc.queue;

import java.io.EOFException;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.SynchronousQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousReader;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.bean.tuple.Pair;

@NotThreadSafe
public class QueueSynchronousReader implements ISynchronousReader {

    private Queue<Pair<Integer, byte[]>> queue;

    public QueueSynchronousReader(final Queue<Pair<Integer, byte[]>> queue) {
        Assertions.assertThat(queue)
                .as("this implementation does not support non-blocking calls")
                .isNotInstanceOf(SynchronousQueue.class);
        this.queue = queue;
    }

    @Override
    public void open() throws IOException {}

    @Override
    public void close() throws IOException {
        queue = null;
    }

    @Override
    public boolean hasNext() throws IOException {
        return !queue.isEmpty();
    }

    @Override
    public Pair<Integer, byte[]> readMessage() throws IOException {
        final Pair<Integer, byte[]> message = queue.remove();
        if (message == QueueSynchronousWriter.CLOSED_MESSAGE) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

}
