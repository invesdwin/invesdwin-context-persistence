package de.invesdwin.context.persistence.timeseries.ipc.queue.blocking;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousWriter;
import de.invesdwin.context.persistence.timeseries.ipc.SynchronousResponse;
import de.invesdwin.context.persistence.timeseries.ipc.queue.QueueSynchronousWriter;

@NotThreadSafe
public class BlockingQueueSynchronousWriter extends ABlockingQueueSynchronousChannel implements ISynchronousWriter {

    public BlockingQueueSynchronousWriter(final BlockingQueue<SynchronousResponse> queue) {
        super(queue);
    }

    @Override
    public void write(final int type, final int sequence, final byte[] message) throws IOException {
        final SynchronousResponse closedMessage = queue.poll();
        if (closedMessage != null) {
            if (closedMessage != QueueSynchronousWriter.CLOSED_MESSAGE) {
                throw new IllegalStateException("Multiple writers on queue are not supported!");
            } else {
                close();
                return;
            }
        }

        try {
            queue.put(new SynchronousResponse(type, sequence, message));
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
