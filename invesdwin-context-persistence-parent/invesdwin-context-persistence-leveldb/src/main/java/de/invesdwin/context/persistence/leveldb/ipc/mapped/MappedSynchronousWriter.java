package de.invesdwin.context.persistence.leveldb.ipc.mapped;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.leveldb.ipc.ISynchronousWriter;

/**
 * There should only be one writer per file, or else the threads might destroy each others data.
 *
 */
@ThreadSafe
public class MappedSynchronousWriter extends AMappedSynchronousChannel implements ISynchronousWriter {

    public MappedSynchronousWriter(final File file, final long maxMessageSize) {
        super(file, maxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        //maybe remove closed flag that causes IOException on reader
        setTransaction(TRANSACTION_INITIAL_VALUE);
    }

    /**
     * Writes a message.
     *
     * @param message
     *            the message object to write
     * @throws EOFException
     *             in case the end of the file was reached
     */
    @Override
    public void write(final int type, final byte[] message) {
        final byte nextTransaction = getNextTransaction();
        //open transaction
        setTransaction(TRANSACTION_WRITING_VALUE);

        setType(type);

        setMessage(message);

        //commit
        setTransaction(nextTransaction);
    }

    @Override
    public void close() throws IOException {
        setTransaction(TRANSACTION_CLOSED_VALUE);
        super.close();
    }

}