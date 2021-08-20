package de.invesdwin.context.persistence.timeseries.ipc.socket;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousChannel;
import de.invesdwin.context.persistence.timeseries.ipc.socket.udp.ADatagramSocketSynchronousChannel;
import de.invesdwin.util.time.duration.Duration;
import ezdb.serde.IntegerSerde;
import ezdb.serde.Serde;

@NotThreadSafe
public abstract class ASocketSynchronousChannel implements ISynchronousChannel {

    public static final int TYPE_POS = 0;
    public static final Serde<Integer> TYPE_SERDE = IntegerSerde.get;
    public static final int TYPE_OFFSET = TYPE_SERDE.toBytes(Integer.MAX_VALUE).length;
    public static final byte TYPE_CLOSED_VALUE = -1;

    public static final int SEQUENCE_POS = TYPE_POS + TYPE_OFFSET;
    public static final Serde<Integer> SEQUENCE_SERDE = TYPE_SERDE;
    public static final int SEQUENCE_OFFSET = TYPE_OFFSET;
    public static final byte SEQUENCE_CLOSED_VALUE = -1;

    public static final int SIZE_POS = SEQUENCE_POS + SEQUENCE_OFFSET;
    public static final Serde<Integer> SIZE_SERDE = SEQUENCE_SERDE;
    public static final int SIZE_OFFSET = SEQUENCE_OFFSET;

    public static final int MESSAGE_POS = SIZE_POS + SIZE_OFFSET;

    protected final int maxMessageSize;
    protected final int bufferSize;
    protected Socket socket;
    private final SocketAddress socketAddress;
    private final boolean server;
    private ServerSocket serverSocket;

    public ASocketSynchronousChannel(final SocketAddress socketAddress, final boolean server,
            final int maxMessageSize) {
        this.socketAddress = socketAddress;
        this.server = server;
        this.maxMessageSize = maxMessageSize;
        this.bufferSize = maxMessageSize + MESSAGE_POS;
    }

    @Override
    public void open() throws IOException {
        if (server) {
            serverSocket = new ServerSocket();
            serverSocket.bind(socketAddress);
            socket = serverSocket.accept();
        } else {
            for (int tries = 0;; tries++) {
                try {
                    socket = new Socket();
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
        socket.setTrafficClass(
                ADatagramSocketSynchronousChannel.IPTOS_LOWDELAY | ADatagramSocketSynchronousChannel.IPTOS_THROUGHPUT);
        socket.setReceiveBufferSize(bufferSize);
        socket.setSendBufferSize(bufferSize);
        socket.setTcpNoDelay(true);
    }

    protected Duration getConnectRetryDelay() {
        return Duration.ONE_SECOND;
    }

    protected int getMaxConnectRetries() {
        return 10;
    }

    @Override
    public void close() throws IOException {
        if (socket != null) {
            socket.close();
            socket = null;
        }
        if (serverSocket != null) {
            serverSocket.close();
            serverSocket = null;
        }
    }

    protected EOFException newEofException(final IOException e) throws EOFException {
        final EOFException eof = new EOFException(e.getMessage());
        eof.initCause(e);
        return eof;
    }

}
