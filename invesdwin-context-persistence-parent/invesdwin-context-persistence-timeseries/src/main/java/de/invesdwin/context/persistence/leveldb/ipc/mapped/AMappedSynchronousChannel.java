package de.invesdwin.context.persistence.leveldb.ipc.mapped;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.leveldb.ipc.ISynchronousChannel;
import ezdb.serde.IntegerSerde;
import ezdb.serde.LongSerde;

@NotThreadSafe
public abstract class AMappedSynchronousChannel implements ISynchronousChannel {

    public static final long TRANSACTION_POS = 0;
    public static final int TRANSACTION_OFFSET = 1;
    public static final byte TRANSACTION_INITIAL_VALUE = 0;
    public static final byte TRANSACTION_WRITING_VALUE = -1;
    public static final byte TRANSACTION_CLOSED_VALUE = -2;

    public static final long TYPE_POS = TRANSACTION_POS + TRANSACTION_OFFSET;
    public static final int TYPE_OFFSET = IntegerSerde.get.toBytes(Integer.MAX_VALUE).length;

    public static final long SIZE_POS = TYPE_POS + TYPE_OFFSET;
    public static final int SIZE_OFFSET = LongSerde.get.toBytes(Long.MAX_VALUE).length;

    public static final long MESSAGE_POS = SIZE_POS + SIZE_OFFSET;
    public static final int MIN_PHYSICAL_MESSAGE_SIZE = 4096 - (int) MESSAGE_POS;

    protected MemoryMappedFile mem;
    protected final File file;
    private final int maxMessageSize;

    public AMappedSynchronousChannel(final File file, final int maxMessageSize) {
        this.file = file;
        if (maxMessageSize <= 0) {
            throw new IllegalArgumentException("fileSize needs to be positive");
        }
        this.maxMessageSize = maxMessageSize;
    }

    @Override
    public void open() throws IOException {
        final long fileSize = maxMessageSize + MESSAGE_POS;
        try {
            this.mem = new MemoryMappedFile(file.getAbsolutePath(), fileSize);
        } catch (final Exception e) {
            throw new IOException("Unable to open file: " + file, e);
        }
    }

    protected byte getNextTransaction() {
        byte transaction = getTransaction();
        if (transaction == TRANSACTION_WRITING_VALUE) {
            throw new IllegalStateException(
                    "Someone else seems is writing a transaction, exclusive file access is needed!");
        }
        do {
            transaction++;
        } while (transaction == TRANSACTION_WRITING_VALUE || transaction == TRANSACTION_CLOSED_VALUE
                || transaction == TRANSACTION_INITIAL_VALUE);
        return transaction;
    }

    protected void setTransaction(final byte val) {
        mem.putByteVolatile(TRANSACTION_POS, val);
    }

    protected byte getTransaction() {
        return mem.getByteVolatile(TRANSACTION_POS);
    }

    protected void setType(final int val) {
        mem.putInt(TYPE_POS, val);
    }

    protected int getType() {
        return mem.getInt(TYPE_POS);
    }

    private void setSize(final int val) {
        if (val > maxMessageSize) {
            throw new IllegalStateException(
                    "messageSize [" + val + "] exceeds maxMessageSize [" + maxMessageSize + "]");
        }
        mem.putInt(SIZE_POS, val);
    }

    private int getSize() {
        return mem.getInt(SIZE_POS);
    }

    protected byte[] getMessage() {
        final int size = getSize();
        final byte[] data = new byte[size];
        mem.getBytes(MESSAGE_POS, data, 0, size);
        return data;
    }

    protected void setMessage(final byte[] data) {
        final int size = data.length;
        setSize(size);
        mem.setBytes(MESSAGE_POS, data, 0, size);
    }

    @Override
    public void close() throws IOException {
        if (mem != null) {
            try {
                mem.unmap();
                mem = null;
            } catch (final Exception e) {
                throw new IOException("Unable to close the file: " + file, e);
            }
        }
    }

}
