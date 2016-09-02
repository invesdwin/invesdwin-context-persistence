package de.invesdwin.context.persistence.leveldb.ipc.pipe;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.lts.ipc.IPCException;

import de.invesdwin.context.persistence.leveldb.ipc.ISynchronousWriter;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class PipeSynchronousWriter extends APipeSynchronousChannel implements ISynchronousWriter {

    public PipeSynchronousWriter(final File file, final Duration blockingTimeout) {
        super(file, blockingTimeout);
    }

    @Override
    public void open() throws IOException {
        super.open();
        try {
            fifo.create();
            fifo.openWriter();
        } catch (final IPCException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void write(final int type, final byte[] message) throws IOException {
        try {
            final byte[] typeBuffer = TYPE_SERDE.toBytes(type);
            fifo.write(typeBuffer);
            final byte[] sizeBuffer = SIZE_SERDE.toBytes(message.length);
            fifo.write(sizeBuffer);
            fifo.write(message);
        } catch (final IPCException e) {
            throw new IOException(e);
        }
    }

}
