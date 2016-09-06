package de.invesdwin.context.persistence.leveldb.ipc.socket.udp;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.leveldb.ipc.ISynchronousChannel;
import de.invesdwin.util.time.duration.Duration;
import ezdb.serde.IntegerSerde;
import ezdb.serde.Serde;

@NotThreadSafe
public abstract class ADatagramSocketSynchronousChannel implements ISynchronousChannel {

    public static final int TYPE_POS = 0;
    public static final Serde<Integer> TYPE_SERDE = IntegerSerde.get;
    public static final int TYPE_OFFSET = TYPE_SERDE.toBytes(Integer.MAX_VALUE).length;
    public static final byte TYPE_CLOSED_VALUE = -1;

    public static final int SIZE_POS = TYPE_POS + TYPE_OFFSET;
    public static final Serde<Integer> SIZE_SERDE = TYPE_SERDE;
    public static final int SIZE_OFFSET = TYPE_OFFSET;

    public static final int MESSAGE_POS = SIZE_POS + SIZE_OFFSET;

    protected final int maxMessageSize;
    protected final int bufferSize;
    protected final byte[] packetBytes;
    protected ByteBuffer packetBuffer;
    protected final DatagramPacket packet;
    protected DatagramSocket socket;
    private final SocketAddress socketAddress;
    private final boolean server;

    public ADatagramSocketSynchronousChannel(final SocketAddress socketAddress, final boolean server,
            final int maxMessageSize) {
        this.socketAddress = socketAddress;
        this.server = server;
        this.maxMessageSize = maxMessageSize;
        this.bufferSize = maxMessageSize + MESSAGE_POS;
        this.packetBytes = new byte[bufferSize];
        this.packetBuffer = ByteBuffer.wrap(packetBytes);
        this.packet = new DatagramPacket(packetBytes, packetBytes.length);
    }

    @Override
    public void open() throws IOException {
        if (server) {
            socket = new DatagramSocket(socketAddress);
        } else {
            for (int tries = 0;; tries++) {
                try {
                    socket = new DatagramSocket();
                    socket.connect(socketAddress);
                    break;
                } catch (final ConnectException e) {
                    socket.close();
                    socket = null;
                    if (tries < getMaxConnectRetries()) {
                        try {
                            getConnectRetryDelay().sleep();
                        } catch (final InterruptedException e1) {
                            throw new RuntimeException(e1);
                        }
                    } else {
                        throw e;
                    }
                }
            }
        }
        socket.setSendBufferSize(bufferSize);
        socket.setReceiveBufferSize(bufferSize);
    }

    protected Duration getConnectRetryDelay() {
        return Duration.ONE_SECOND;
    }

    protected int getMaxConnectRetries() {
        return 10;
    }

    protected void setType(final int val) {
        packetBuffer.putInt(TYPE_POS, val);
    }

    protected int getType() {
        return packetBuffer.getInt(TYPE_POS);
    }

    private void setSize(final int val) {
        if (val > maxMessageSize) {
            throw new IllegalStateException(
                    "messageSize [" + val + "] exceeds maxMessageSize [" + maxMessageSize + "]");
        }
        packetBuffer.putInt(SIZE_POS, val);
    }

    private int getSize() {
        return packetBuffer.getInt(SIZE_POS);
    }

    protected byte[] getMessage() {
        final int size = getSize();
        final byte[] data = new byte[size];
        System.arraycopy(packetBytes, MESSAGE_POS, data, 0, size);
        return data;
    }

    protected void setMessage(final byte[] data) {
        final int size = data.length;
        setSize(size);
        System.arraycopy(data, 0, packetBytes, MESSAGE_POS, size);
        packet.setLength(MESSAGE_POS + size);
    }

    @Override
    public void close() throws IOException {
        if (socket != null) {
            socket.close();
            socket = null;
        }
    }

    protected EOFException newEofException(final IOException e) throws EOFException {
        final EOFException eof = new EOFException(e.getMessage());
        eof.initCause(e);
        return eof;
    }

}
