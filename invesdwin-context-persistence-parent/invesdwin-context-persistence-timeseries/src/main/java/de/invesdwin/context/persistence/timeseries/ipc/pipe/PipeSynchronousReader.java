package de.invesdwin.context.persistence.timeseries.ipc.pipe;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousReader;
import de.invesdwin.context.persistence.timeseries.ipc.SynchronousResponse;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
public class PipeSynchronousReader extends APipeSynchronousChannel implements ISynchronousReader {

    private static final int CLOSED_READ_COUNT = -1;
    private static final int TIMEOUT_READ_COUNT = 0;
    private final byte[] typeBuffer = new byte[TYPE_OFFSET];
    private final byte[] sequenceBuffer = new byte[SEQUENCE_OFFSET];
    private final byte[] sizeBuffer = new byte[SIZE_OFFSET];
    private BufferedInputStream in;

    public PipeSynchronousReader(final File file, final int maxMessageSize) {
        super(file, maxMessageSize);
    }

    @Override
    public void open() throws IOException {
        in = new BufferedInputStream(new FileInputStream(file), fileSize);
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public boolean hasNext() throws IOException {
        try {
            return in.available() >= TYPE_OFFSET;
        } catch (final IOException e) {
            throw newEofException(e);
        }
    }

    @Override
    public SynchronousResponse readMessage() throws IOException {
        Assertions.checkTrue(read(typeBuffer));
        final int type = TYPE_SERDE.fromBytes(typeBuffer);
        if (type == TYPE_CLOSED_VALUE) {
            throw new EOFException("Channel was closed by the other endpoint");
        }
        Assertions.checkTrue(read(sequenceBuffer));
        final int sequence = SEQUENCE_SERDE.fromBytes(sequenceBuffer);
        Assertions.checkTrue(read(sizeBuffer));
        final int size = SIZE_SERDE.fromBytes(sizeBuffer);
        final byte[] message = new byte[size];
        if (size > 0) {
            Assertions.checkTrue(read(message));
        }
        return new SynchronousResponse(type, sequence, message);
    }

    private boolean read(final byte[] buffer) throws IOException {
        try {
            if (in.available() <= 0) {
                return false;
            }
        } catch (final IOException e) {
            throw newEofException(e);
        }
        final int read = in.read(buffer);
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
    }

}
