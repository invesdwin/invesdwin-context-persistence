package de.invesdwin.context.persistence.leveldb.ipc.pipe;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.leveldb.ipc.ISynchronousWriter;

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
        out.close();
    }

    private void checkSize(final int size) {
        if (size > maxMessageSize) {
            throw new IllegalStateException(
                    "messageSize [" + size + "] exceeds maxMessageSize [" + maxMessageSize + "]");
        }
    }

    @Override
    public void write(final int type, final byte[] message) throws IOException {
        checkSize(message.length);
        final byte[] typeBuffer = TYPE_SERDE.toBytes(type);
        out.write(typeBuffer);
        final byte[] sizeBuffer = SIZE_SERDE.toBytes(message.length);
        out.write(sizeBuffer);
        if (message.length > 0) {
            out.write(message);
        }
        out.flush();
    }

}
