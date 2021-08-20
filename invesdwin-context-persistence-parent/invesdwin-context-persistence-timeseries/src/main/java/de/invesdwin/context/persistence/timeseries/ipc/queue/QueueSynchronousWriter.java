package de.invesdwin.context.persistence.timeseries.ipc.queue;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.SynchronousQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousWriter;
import de.invesdwin.context.persistence.timeseries.ipc.response.ClosedSynchronousResponse;
import de.invesdwin.context.persistence.timeseries.ipc.response.ISynchronousResponse;
import de.invesdwin.context.persistence.timeseries.ipc.response.SynchronousResponse;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
public class QueueSynchronousWriter<M> implements ISynchronousWriter<M> {

    private Queue<ISynchronousResponse<M>> queue;

    public QueueSynchronousWriter(final Queue<ISynchronousResponse<M>> queue) {
        Assertions.assertThat(queue)
                .as("this implementation does not support non-blocking calls")
                .isNotInstanceOf(SynchronousQueue.class);
        this.queue = queue;
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public void close() throws IOException {
        queue.add(ClosedSynchronousResponse.getInstance());
        queue = null;
    }

    @Override
    public void write(final int type, final int sequence, final M message) throws IOException {
        queue.add(new SynchronousResponse<M>(type, sequence, message));
    }

    @Override
    public void write(final ISynchronousResponse<M> response) throws IOException {
        queue.add(response);
    }

}
