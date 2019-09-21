package de.invesdwin.context.persistence.timeseries.ipc.queue;

import java.io.EOFException;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.SynchronousQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousReader;
import de.invesdwin.context.persistence.timeseries.ipc.SynchronousResponse;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
public class QueueSynchronousReader implements ISynchronousReader {

    private Queue<SynchronousResponse> queue;

    public QueueSynchronousReader(final Queue<SynchronousResponse> queue) {
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
    public SynchronousResponse readMessage() throws IOException {
        final SynchronousResponse message = queue.remove();
        if (message == QueueSynchronousWriter.CLOSED_MESSAGE) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

}
