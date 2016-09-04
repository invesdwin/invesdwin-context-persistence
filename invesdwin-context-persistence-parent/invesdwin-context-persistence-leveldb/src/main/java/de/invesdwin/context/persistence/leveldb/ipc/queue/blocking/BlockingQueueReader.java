package de.invesdwin.context.persistence.leveldb.ipc.queue.blocking;

import java.io.EOFException;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.leveldb.ipc.ISynchronousReader;
import de.invesdwin.context.persistence.leveldb.ipc.queue.QueueSynchronousWriter;
import de.invesdwin.util.bean.tuple.Pair;

@NotThreadSafe
public class BlockingQueueReader implements ISynchronousReader {
    private Queue<Pair<Integer, byte[]>> queue;

    public BlockingQueueReader(final BlockingQueue<Pair<Integer, byte[]>> queue) {
        this.queue = queue;
    }

    @Override
    public void open() throws IOException {}

    @Override
    public void close() throws IOException {
        queue = null;
    }

    @Override
    public boolean hasNext() throws IOException {
        return !queue.isEmpty();
    }

    @Override
    public Pair<Integer, byte[]> readMessage() throws IOException {
        final Pair<Integer, byte[]> message = queue.remove();
        if (message == QueueSynchronousWriter.CLOSED_MESSAGE) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

}
