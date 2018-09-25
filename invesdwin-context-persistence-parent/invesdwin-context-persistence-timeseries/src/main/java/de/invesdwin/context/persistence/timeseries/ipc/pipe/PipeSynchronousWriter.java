package de.invesdwin.context.persistence.timeseries.ipc.pipe;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousWriter;
import de.invesdwin.util.math.Bytes;

@NotThreadSafe
public class PipeSynchronousWriter extends APipeSynchronousChannel implements ISynchronousWriter {

    private BufferedOutputStream out;

    public PipeSynchronousWriter(final File file, final int maxMessageSize) {
        super(file, maxMessageSize);
    }

    @Override
    public void open() throws IOException {
        out = new BufferedOutputStream(new FileOutputStream(file, true), fileSize);
    }

    @Override
    public void close() throws IOException {
        try {
            writeWithoutTypeCheck(TYPE_CLOSED_VALUE, Bytes.EMPTY_ARRAY);
        } catch (final Throwable t) {
            //ignore
        }
        try {
            out.close();
        } catch (final Throwable t) {
            //ignore
        }
    }

    private void checkSize(final int size) {
        if (size > maxMessageSize) {
            throw new IllegalStateException(
                    "messageSize [" + size + "] exceeds maxMessageSize [" + maxMessageSize + "]");
        }
    }

    private void checkType(final int type) {
        if (type == TYPE_CLOSED_VALUE) {
            throw new IllegalArgumentException(
                    "type [" + type + "] is reserved for close notification, please use a different type number");
        }
    }

    @Override
    public void write(final int type, final byte[] message) throws IOException {
        checkType(type);
        writeWithoutTypeCheck(type, message);
    }

    private void writeWithoutTypeCheck(final int type, final byte[] message) throws IOException {
        checkSize(message.length);
        final byte[] typeBuffer = TYPE_SERDE.toBytes(type);
        out.write(typeBuffer);
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
