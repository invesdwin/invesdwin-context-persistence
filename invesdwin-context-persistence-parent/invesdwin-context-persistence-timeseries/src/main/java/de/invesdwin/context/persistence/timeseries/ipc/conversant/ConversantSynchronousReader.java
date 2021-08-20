package de.invesdwin.context.persistence.timeseries.ipc.conversant;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.SynchronousQueue;

import javax.annotation.concurrent.NotThreadSafe;

import com.conversantmedia.util.concurrent.ConcurrentQueue;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousReader;
import de.invesdwin.context.persistence.timeseries.ipc.response.EmptySynchronousResponse;
import de.invesdwin.context.persistence.timeseries.ipc.response.ISynchronousResponse;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
public class ConversantSynchronousReader<M> implements ISynchronousReader<M> {

    private ConcurrentQueue<ISynchronousResponse<M>> queue;

    public ConversantSynchronousReader(final ConcurrentQueue<ISynchronousResponse<M>> queue) {
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
        return !queue.isEmpty();
    }

    @Override
    public ISynchronousResponse<M> readMessage() throws IOException {
        final ISynchronousResponse<M> message = queue.poll();
        if (message == EmptySynchronousResponse.getInstance()) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

}
