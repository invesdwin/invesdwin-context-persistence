package de.invesdwin.context.persistence.timeseries.ipc.conversant;

import java.io.IOException;
import java.util.concurrent.SynchronousQueue;

import javax.annotation.concurrent.NotThreadSafe;

import com.conversantmedia.util.concurrent.ConcurrentQueue;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousWriter;
import de.invesdwin.context.persistence.timeseries.ipc.response.EmptySynchronousResponse;
import de.invesdwin.context.persistence.timeseries.ipc.response.ISynchronousResponse;
import de.invesdwin.context.persistence.timeseries.ipc.response.ImmutableSynchronousResponse;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
public class ConversantSynchronousWriter<M> implements ISynchronousWriter<M> {

    private ConcurrentQueue<ISynchronousResponse<M>> queue;

    public ConversantSynchronousWriter(final ConcurrentQueue<ISynchronousResponse<M>> queue) {
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
        queue.offer(EmptySynchronousResponse.getInstance());
        queue = null;
    }

    @Override
    public void write(final int type, final int sequence, final M message) throws IOException {
        queue.offer(new ImmutableSynchronousResponse<M>(type, sequence, message));
    }

    @Override
    public void write(final ISynchronousResponse<M> response) throws IOException {
        queue.offer(response);
    }

}
