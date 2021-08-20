package de.invesdwin.context.persistence.timeseries.ipc.chronicle;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousReader;
import de.invesdwin.context.persistence.timeseries.ipc.message.EmptySynchronousMessage;
import de.invesdwin.context.persistence.timeseries.ipc.message.ISynchronousMessage;
import de.invesdwin.context.persistence.timeseries.ipc.message.ImmutableSynchronousMessage;
import de.invesdwin.util.math.Bytes;
import net.openhft.chronicle.queue.ExcerptTailer;

@NotThreadSafe
public class ChronicleSynchronousReader extends AChronicleSynchronousChannel implements ISynchronousReader<byte[]> {

    private ExcerptTailer tailer;
    private net.openhft.chronicle.bytes.Bytes<?> bytes;

    public ChronicleSynchronousReader(final File file) {
        super(file);
    }

    @Override
    public void open() throws IOException {
        super.open();
        this.tailer = queue.createTailer();
        this.bytes = net.openhft.chronicle.bytes.Bytes.elasticByteBuffer();
    }

    @Override
    public void close() throws IOException {
        if (tailer != null) {
            tailer.close();
            tailer = null;
            bytes = null;
        }
        super.close();
    }

    @Override
    public boolean hasNext() throws IOException {
        return tailer.readBytes(bytes);
    }

    @Override
    public ISynchronousMessage<byte[]> readMessage() throws IOException {
        final int type = bytes.readInt();
        if (type == EmptySynchronousMessage.TYPE) {
            close();
            throw new EOFException("closed by other side");
        }
        final int sequence = bytes.readInt();
        final int size = bytes.readInt();
        final byte[] message;
        if (size == 0) {
            message = Bytes.EMPTY_ARRAY;
        } else {
            message = new byte[size];
            bytes.read(message);
        }
        return new ImmutableSynchronousMessage<byte[]>(type, sequence, message);
    }

}
