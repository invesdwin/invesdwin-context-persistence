package de.invesdwin.context.persistence.timeseries.ipc.queue;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.SynchronousQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousWriter;
import de.invesdwin.context.persistence.timeseries.ipc.SynchronousResponse;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
public class QueueSynchronousWriter implements ISynchronousWriter {

    public static final SynchronousResponse CLOSED_MESSAGE = new SynchronousResponse(-1, -1, null);
    private Queue<SynchronousResponse> queue;

    public QueueSynchronousWriter(final Queue<SynchronousResponse> queue) {
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
    public void write(final int type, final int sequence, final byte[] message) throws IOException {
        queue.add(new SynchronousResponse(type, sequence, message));
    }

}
