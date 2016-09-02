package de.invesdwin.context.persistence.leveldb.ipc.mapped;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.leveldb.ipc.ISynchronousReader;
import de.invesdwin.util.bean.tuple.Pair;

/**
 * There can be multiple readers per file, but it is better to only have one.
 * 
 * Excerpt from SynchronousQueue: Blocking is mainly accomplished using LockSupport park/unpark, except that nodes that
 * appear to be the next ones to become fulfilled first spin a bit (on multiprocessors only). On very busy synchronous
 * queues, spinning can dramatically improve throughput. And on less busy ones, the amount of spinning is small enough
 * not to be noticeable.
 * 
 * @author subes
 *
 */
@NotThreadSafe
public class MappedSynchronousReader extends AMappedSynchronousChannel implements ISynchronousReader {
    private int lastTransaction;

    public MappedSynchronousReader(final File file, final int maxMessageSize) {
        super(file, maxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        lastTransaction = getTransaction();
    }

    @Override
    /**
     * This method is thread safe and does not require locking on the reader.
     */
    public boolean hasNext() throws IOException {
        if (mem == null) {
            return false;
        }
        final int curTransaction = getTransaction();
        if (curTransaction == TRANSACTION_CLOSED_VALUE) {
            throw new EOFException("Channel was closed by the other endpoint");
        }
        return curTransaction != lastTransaction && curTransaction != TRANSACTION_WRITING_VALUE
                && curTransaction != TRANSACTION_INITIAL_VALUE;
    }

    @Override
    public Pair<Integer, byte[]> readMessage() {
        lastTransaction = getTransaction();
        return Pair.of(getType(), getMessage());
    }

}