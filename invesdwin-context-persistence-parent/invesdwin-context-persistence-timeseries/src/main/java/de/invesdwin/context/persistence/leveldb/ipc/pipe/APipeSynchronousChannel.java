package de.invesdwin.context.persistence.leveldb.ipc.pipe;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.leveldb.ipc.ISynchronousChannel;
import ezdb.serde.IntegerSerde;
import ezdb.serde.Serde;

@NotThreadSafe
public abstract class APipeSynchronousChannel implements ISynchronousChannel {

    public static final int TYPE_POS = 0;
    public static final Serde<Integer> TYPE_SERDE = IntegerSerde.get;
    public static final int TYPE_OFFSET = TYPE_SERDE.toBytes(Integer.MAX_VALUE).length;
    public static final byte TYPE_CLOSED_VALUE = -1;

    public static final int SIZE_POS = TYPE_POS + TYPE_OFFSET;
    public static final Serde<Integer> SIZE_SERDE = TYPE_SERDE;
    public static final int SIZE_OFFSET = TYPE_OFFSET;

    public static final int MESSAGE_POS = SIZE_POS + SIZE_OFFSET;

    protected final File file;
    protected final int maxMessageSize;
    protected final int fileSize;

    public APipeSynchronousChannel(final File file, final int maxMessageSize) {
        this.file = file;
        this.maxMessageSize = maxMessageSize;
        this.fileSize = maxMessageSize + MESSAGE_POS;
    }

    protected EOFException newEofException(final IOException e) throws EOFException {
        final EOFException eof = new EOFException(e.getMessage());
        eof.initCause(e);
        return eof;
    }

}
