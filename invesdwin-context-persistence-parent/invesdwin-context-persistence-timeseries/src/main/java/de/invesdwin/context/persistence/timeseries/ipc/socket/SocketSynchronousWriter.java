package de.invesdwin.context.persistence.timeseries.ipc.socket;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousWriter;
import de.invesdwin.context.persistence.timeseries.ipc.message.ISynchronousMessage;
import de.invesdwin.util.math.Bytes;

@NotThreadSafe
public class SocketSynchronousWriter extends ASocketSynchronousChannel implements ISynchronousWriter<byte[]> {

    private BufferedOutputStream out;

    public SocketSynchronousWriter(final SocketAddress socketAddress, final boolean server, final int maxMessageSize) {
        super(socketAddress, server, maxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        out = new BufferedOutputStream(socket.getOutputStream(), bufferSize);
    }

    @Override
    public void close() throws IOException {
        if (out != null) {
            try {
                writeWithoutTypeCheck(TYPE_CLOSED_VALUE, SEQUENCE_CLOSED_VALUE, Bytes.EMPTY_ARRAY);
            } catch (final Throwable t) {
                //ignore
            }
            try {
                out.close();
            } catch (final Throwable t) {
                //ignore
            }
            out = null;
        }
        super.close();
    }

    private void checkType(final int type) {
        if (type == TYPE_CLOSED_VALUE) {
            throw new IllegalArgumentException(
                    "type [" + type + "] is reserved for close notification, please use a different type number");
        }
    }

    private void checkSize(final int size) {
        if (size > maxMessageSize) {
            throw new IllegalStateException(
                    "messageSize [" + size + "] exceeds maxMessageSize [" + maxMessageSize + "]");
        }
    }

    @Override
    public void write(final int type, final int sequence, final byte[] message) throws IOException {
        checkType(type);
        writeWithoutTypeCheck(type, sequence, message);
    }

    @Override
    public void write(final ISynchronousMessage<byte[]> message) throws IOException {
        write(message.getType(), message.getSequence(), message.getMessage());
    }

    private void writeWithoutTypeCheck(final int type, final int sequence, final byte[] message) throws IOException {
        checkSize(message.length);
        final byte[] typeBuffer = TYPE_SERDE.toBytes(type);
        out.write(typeBuffer);
        final byte[] sequenceBuffer = SEQUENCE_SERDE.toBytes(sequence);
        out.write(sequenceBuffer);
        final byte[] sizeBuffer = SIZE_SERDE.toBytes(message.length);
        out.write(sizeBuffer);
        if (message.length > 0) {
            out.write(message);
        }
        try {
            out.flush();
        } catch (final IOException e) {
            throw newEofException(e);
        }
    }
}
