package de.invesdwin.context.persistence.timeseries.ipc.aeron;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousWriter;
import de.invesdwin.context.persistence.timeseries.ipc.message.EmptySynchronousMessage;
import de.invesdwin.context.persistence.timeseries.ipc.message.ISynchronousMessage;
import de.invesdwin.util.time.date.FTimeUnit;
import io.aeron.ConcurrentPublication;
import io.aeron.Publication;

@NotThreadSafe
public class AeronSynchronousWriter extends AAeronSynchronousChannel implements ISynchronousWriter<byte[]> {

    private ConcurrentPublication publication;
    private MutableDirectBuffer buffer;
    private boolean connected;

    public AeronSynchronousWriter(final String channel, final int streamId) {
        super(channel, streamId);
    }

    @Override
    public void open() throws IOException {
        super.open();
        this.publication = aeron.addPublication(channel, streamId);
        this.buffer = new ExpandableArrayBuffer();
        this.connected = false;
    }

    @Override
    public void close() throws IOException {
        if (publication != null) {
            if (connected) {
                try {
                    write(EmptySynchronousMessage.getInstance());
                } catch (final Throwable t) {
                    //ignore
                }
            }
            if (publication != null) {
                publication.close();
                publication = null;
                buffer = null;
                this.connected = false;
            }
        }
        super.close();
    }

    @Override
    public void write(final int type, final int sequence, final byte[] message) throws IOException {
        buffer.putInt(TYPE_INDEX, type);
        buffer.putInt(SEQUENCE_INDEX, sequence);
        final int size;
        if (message == null || message.length == 0) {
            size = MESSAGE_INDEX;
        } else {
            buffer.putBytes(MESSAGE_INDEX, message);
            size = MESSAGE_INDEX + message.length;
        }
        sendRetrying(size);
    }

    private void sendRetrying(final int size) throws IOException, EOFException, InterruptedIOException {
        while (!sendTry(size)) {
            try {
                FTimeUnit.MILLISECONDS.sleep(1);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                final InterruptedIOException interrupt = new InterruptedIOException(e.getMessage());
                interrupt.initCause(e);
                throw interrupt;
            }
        }
        connected = true;
    }

    private boolean sendTry(final int size) throws IOException, EOFException {
        final long result = publication.offer(buffer, 0, size);
        if (result <= 0) {
            if (result == Publication.NOT_CONNECTED) {
                if (connected) {
                    connected = false;
                    close();
                    throw new EOFException("closed by other side: NOT_CONNECTED=" + result);
                } else {
                    return false;
                }
            } else if (result == Publication.CLOSED) {
                close();
                throw new EOFException("closed by other side: CLOSED=" + result);
            } else if (result == Publication.MAX_POSITION_EXCEEDED) {
                close();
                throw new EOFException("closed by other side: MAX_POSITION_EXCEEDED=" + result);
            } else if (result == Publication.BACK_PRESSURED || result == Publication.ADMIN_ACTION) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void write(final ISynchronousMessage<byte[]> message) throws IOException {
        write(message.getType(), message.getSequence(), message.getMessage());
    }

}
