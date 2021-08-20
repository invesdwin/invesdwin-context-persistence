package de.invesdwin.context.persistence.timeseries.ipc.queue.blocking;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousReader;
import de.invesdwin.context.persistence.timeseries.ipc.response.ClosedSynchronousResponse;
import de.invesdwin.context.persistence.timeseries.ipc.response.ISynchronousResponse;

@NotThreadSafe
public class BlockingQueueSynchronousReader<M> extends ABlockingQueueSynchronousChannel<M>
        implements ISynchronousReader<M> {
    ;

    private ISynchronousResponse<M> next;

    public BlockingQueueSynchronousReader(final BlockingQueue<ISynchronousResponse<M>> queue) {
        super(queue);
    }

    @Override
    public boolean hasNext() throws IOException {
        if (next != null) {
            return true;
        }
        next = queue.poll();
        return next != null;
    }

    @Override
    public ISynchronousResponse<M> readMessage() throws IOException {
        final ISynchronousResponse<M> message;
        message = next;
        next = null;
        if (message == ClosedSynchronousResponse.getInstance()) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

}
