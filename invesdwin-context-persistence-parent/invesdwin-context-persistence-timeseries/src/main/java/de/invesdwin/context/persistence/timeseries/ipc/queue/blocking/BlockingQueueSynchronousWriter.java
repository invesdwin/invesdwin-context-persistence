package de.invesdwin.context.persistence.timeseries.ipc.queue.blocking;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousWriter;
import de.invesdwin.context.persistence.timeseries.ipc.response.EmptySynchronousResponse;
import de.invesdwin.context.persistence.timeseries.ipc.response.ISynchronousResponse;
import de.invesdwin.context.persistence.timeseries.ipc.response.ImmutableSynchronousResponse;

@NotThreadSafe
public class BlockingQueueSynchronousWriter<M> extends ABlockingQueueSynchronousChannel<M>
        implements ISynchronousWriter<M> {

    public BlockingQueueSynchronousWriter(final BlockingQueue<ISynchronousResponse<M>> queue) {
        super(queue);
    }

    @Override
    public void write(final int type, final int sequence, final M message) throws IOException {
        final ISynchronousResponse<M> closedMessage = queue.poll();
        if (closedMessage != null) {
            if (closedMessage != EmptySynchronousResponse.getInstance()) {
                throw new IllegalStateException("Multiple writers on queue are not supported!");
            } else {
                close();
                return;
            }
        }

        try {
            queue.put(new ImmutableSynchronousResponse<M>(type, sequence, message));
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(final ISynchronousResponse<M> response) throws IOException {
        final ISynchronousResponse<M> closedMessage = queue.poll();
        if (closedMessage != null) {
            if (closedMessage != EmptySynchronousResponse.getInstance()) {
                throw new IllegalStateException("Multiple writers on queue are not supported!");
            } else {
                close();
                return;
            }
        }

        try {
            queue.put(response);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
