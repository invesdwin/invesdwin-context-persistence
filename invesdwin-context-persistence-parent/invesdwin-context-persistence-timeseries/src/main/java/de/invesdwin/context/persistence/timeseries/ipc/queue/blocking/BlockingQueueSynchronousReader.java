package de.invesdwin.context.persistence.timeseries.ipc.queue.blocking;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousReader;
import de.invesdwin.context.persistence.timeseries.ipc.message.EmptySynchronousMessage;
import de.invesdwin.context.persistence.timeseries.ipc.message.ISynchronousMessage;

@NotThreadSafe
public class BlockingQueueSynchronousReader<M> extends ABlockingQueueSynchronousChannel<M>
        implements ISynchronousReader<M> {
    ;

    private ISynchronousMessage<M> next;

    public BlockingQueueSynchronousReader(final BlockingQueue<ISynchronousMessage<M>> queue) {
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
    public ISynchronousMessage<M> readMessage() throws IOException {
        final ISynchronousMessage<M> message;
        message = next;
        next = null;
        if (message == EmptySynchronousMessage.getInstance()) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

}
