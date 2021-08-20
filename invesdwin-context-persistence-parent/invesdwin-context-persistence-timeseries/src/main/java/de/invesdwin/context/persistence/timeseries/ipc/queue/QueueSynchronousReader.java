package de.invesdwin.context.persistence.timeseries.ipc.queue;

import java.io.EOFException;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.SynchronousQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousReader;
import de.invesdwin.context.persistence.timeseries.ipc.message.EmptySynchronousMessage;
import de.invesdwin.context.persistence.timeseries.ipc.message.ISynchronousMessage;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
public class QueueSynchronousReader<M> implements ISynchronousReader<M> {

    private Queue<ISynchronousMessage<M>> queue;

    public QueueSynchronousReader(final Queue<ISynchronousMessage<M>> queue) {
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
        queue = null;
    }

    @Override
    public boolean hasNext() throws IOException {
        return queue.peek() != null;
    }

    @Override
    public ISynchronousMessage<M> readMessage() throws IOException {
        final ISynchronousMessage<M> message = queue.remove();
        if (message == EmptySynchronousMessage.getInstance()) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

}
