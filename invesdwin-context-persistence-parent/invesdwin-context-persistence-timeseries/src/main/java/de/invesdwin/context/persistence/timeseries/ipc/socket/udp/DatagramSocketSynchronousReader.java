package de.invesdwin.context.persistence.timeseries.ipc.socket.udp;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousReader;
import de.invesdwin.context.persistence.timeseries.ipc.SynchronousResponse;

@NotThreadSafe
public class DatagramSocketSynchronousReader extends ADatagramSocketSynchronousChannel implements ISynchronousReader {

    public DatagramSocketSynchronousReader(final SocketAddress socketAddress, final int maxMessageSize) {
        super(socketAddress, true, maxMessageSize);
    }

    @Override
    public boolean hasNext() throws IOException {
        socket.receive(packet);
        return true;
    }

    @Override
    public SynchronousResponse readMessage() throws IOException {
        final int type = getType();
        if (type == TYPE_CLOSED_VALUE) {
            throw new EOFException("Channel was closed by the other endpoint");
        }
        final int sequence = getSequence();
        final byte[] message = getMessage();
        return new SynchronousResponse(type, sequence, message);
    }

}
