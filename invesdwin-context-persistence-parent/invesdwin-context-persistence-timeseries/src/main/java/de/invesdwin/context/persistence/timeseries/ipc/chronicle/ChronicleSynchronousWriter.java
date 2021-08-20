package de.invesdwin.context.persistence.timeseries.ipc.chronicle;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousWriter;
import de.invesdwin.context.persistence.timeseries.ipc.message.EmptySynchronousMessage;
import de.invesdwin.context.persistence.timeseries.ipc.message.ISynchronousMessage;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.DocumentContext;

@NotThreadSafe
public class ChronicleSynchronousWriter extends AChronicleSynchronousChannel implements ISynchronousWriter<byte[]> {

    private ExcerptAppender appender;

    public ChronicleSynchronousWriter(final File file) {
        super(file);
    }

    @Override
    public void open() throws IOException {
        super.open();
        appender = queue.acquireAppender();
    }

    @Override
    public void close() throws IOException {
        if (appender != null) {
            write(EmptySynchronousMessage.getInstance());
            appender.close();
            appender = null;
        }
        super.close();
    }

    @Override
    public void write(final int type, final int sequence, final byte[] message) throws IOException {
        try (DocumentContext doc = appender.writingDocument()) {
            final net.openhft.chronicle.bytes.Bytes<?> bytes = doc.wire().bytes();
            bytes.writeInt(type);
            bytes.writeInt(sequence);
            if (message == null) {
                bytes.writeInt(0);
            } else {
                bytes.writeInt(message.length);
                bytes.write(message);
            }
        }
    }

    @Override
    public void write(final ISynchronousMessage<byte[]> message) throws IOException {
        write(message.getType(), message.getSequence(), message.getMessage());
    }

}
