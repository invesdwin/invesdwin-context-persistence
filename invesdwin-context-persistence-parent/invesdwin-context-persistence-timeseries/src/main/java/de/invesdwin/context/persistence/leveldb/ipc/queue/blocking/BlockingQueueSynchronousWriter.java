package de.invesdwin.context.persistence.leveldb.ipc.queue.blocking;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.leveldb.ipc.ISynchronousWriter;
import de.invesdwin.context.persistence.leveldb.ipc.queue.QueueSynchronousWriter;
import de.invesdwin.util.bean.tuple.Pair;

@NotThreadSafe
public class BlockingQueueSynchronousWriter extends ABlockingQueueSynchronousChannel implements ISynchronousWriter {

    public BlockingQueueSynchronousWriter(final BlockingQueue<Pair<Integer, byte[]>> queue) {
        super(queue);
    }

    @Override
    public void write(final int type, final byte[] message) throws IOException {
        final Pair<Integer, byte[]> closedMessage = queue.poll();
        if (closedMessage != null) {
            if (closedMessage != QueueSynchronousWriter.CLOSED_MESSAGE) {
                throw new IllegalStateException("Multiple writers on queue are not supported!");
            } else {
                close();
                return;
            }
        }

        try {
            queue.put(Pair.of(type, message));
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
