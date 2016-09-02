package de.invesdwin.context.persistence.leveldb.ipc.pipe;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.lts.ipc.IPCException;

import de.invesdwin.context.persistence.leveldb.ipc.ISynchronousReader;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.bean.tuple.Pair;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class PipeSynchronousReader extends APipeSynchronousChannel implements ISynchronousReader {

    private static final int CLOSED_READ_COUNT = -1;
    private static final int TIMEOUT_READ_COUNT = 0;
    private final byte[] typeBuffer = new byte[TYPE_OFFSET];
    private final byte[] sizeBuffer = new byte[SIZE_OFFSET];

    public PipeSynchronousReader(final File file, final Duration blockingTimeout) {
        super(file, blockingTimeout);
    }

    @Override
    public void open() throws IOException {
        super.open();
        try {
            fifo.openReader();
        } catch (final IPCException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        return read(typeBuffer);
    }

    @Override
    public Pair<Integer, byte[]> readMessage() throws IOException {
        final int type = TYPE_SERDE.fromBytes(typeBuffer);
        Assertions.checkTrue(read(sizeBuffer));
        final int size = SIZE_SERDE.fromBytes(sizeBuffer);
        final byte[] message = new byte[size];
        Assertions.checkTrue(read(message));
        return Pair.of(type, message);
    }

    private boolean read(final byte[] buffer) throws IOException {
        try {
            final int read = fifo.read(buffer);
            if (read == TIMEOUT_READ_COUNT) {
                return false;
            }
            if (read == CLOSED_READ_COUNT) {
                throw new EOFException("Pipe closed");
            }
            if (read != buffer.length) {
                throw new IllegalStateException("Read less bytes [" + read + "] than expected [" + buffer.length + "]");
            }
            return true;
        } catch (final IPCException e) {
            throw new IOException(e);
        }
    }

}
