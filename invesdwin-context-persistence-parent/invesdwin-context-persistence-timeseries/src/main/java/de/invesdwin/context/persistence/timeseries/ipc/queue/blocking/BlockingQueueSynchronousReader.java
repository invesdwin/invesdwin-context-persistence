package de.invesdwin.context.persistence.timeseries.ipc.queue.blocking;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousReader;
import de.invesdwin.context.persistence.timeseries.ipc.SynchronousResponse;
import de.invesdwin.context.persistence.timeseries.ipc.queue.QueueSynchronousWriter;

@NotThreadSafe
public class BlockingQueueSynchronousReader extends ABlockingQueueSynchronousChannel implements ISynchronousReader {

    private SynchronousResponse next;

    public BlockingQueueSynchronousReader(final BlockingQueue<SynchronousResponse> queue) {
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
    public SynchronousResponse readMessage() throws IOException {
        final SynchronousResponse message;
        message = next;
        next = null;
        if (message == QueueSynchronousWriter.CLOSED_MESSAGE) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

}
