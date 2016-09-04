package de.invesdwin.context.persistence.leveldb.ipc.queue.blocking;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.leveldb.ipc.ISynchronousReader;
import de.invesdwin.context.persistence.leveldb.ipc.queue.QueueSynchronousWriter;
import de.invesdwin.util.bean.tuple.Pair;

@NotThreadSafe
public class BlockingQueueSynchronousReader extends ABlockingQueueSynchronousChannel implements ISynchronousReader {

    private Pair<Integer, byte[]> next;

    public BlockingQueueSynchronousReader(final BlockingQueue<Pair<Integer, byte[]>> queue) {
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
    public Pair<Integer, byte[]> readMessage() throws IOException {
        final Pair<Integer, byte[]> message;
        message = next;
        next = null;
        if (message == QueueSynchronousWriter.CLOSED_MESSAGE) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

}
