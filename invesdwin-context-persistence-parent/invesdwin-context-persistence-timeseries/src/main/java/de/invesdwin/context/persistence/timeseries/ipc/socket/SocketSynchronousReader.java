package de.invesdwin.context.persistence.timeseries.ipc.socket;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousReader;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.bean.tuple.Pair;

@NotThreadSafe
public class SocketSynchronousReader extends ASocketSynchronousChannel implements ISynchronousReader {

    private static final int CLOSED_READ_COUNT = -1;
    private static final int TIMEOUT_READ_COUNT = 0;
    private final byte[] typeBuffer = new byte[TYPE_OFFSET];
    private final byte[] sizeBuffer = new byte[SIZE_OFFSET];
    private BufferedInputStream in;

    public SocketSynchronousReader(final SocketAddress socketAddress, final boolean server, final int maxMessageSize) {
        super(socketAddress, server, maxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        in = new BufferedInputStream(socket.getInputStream(), bufferSize);
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
            in = null;
        }
        super.close();
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
    public Pair<Integer, byte[]> readMessage() throws IOException {
        Assertions.checkTrue(read(typeBuffer));
        final int type = TYPE_SERDE.fromBytes(typeBuffer);
        if (type == TYPE_CLOSED_VALUE) {
            throw new EOFException("Channel was closed by the other endpoint");
        }
        Assertions.checkTrue(read(sizeBuffer));
        final int size = SIZE_SERDE.fromBytes(sizeBuffer);
        final byte[] message = new byte[size];
        if (size > 0) {
            Assertions.checkTrue(read(message));
        }
        return Pair.of(type, message);
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
