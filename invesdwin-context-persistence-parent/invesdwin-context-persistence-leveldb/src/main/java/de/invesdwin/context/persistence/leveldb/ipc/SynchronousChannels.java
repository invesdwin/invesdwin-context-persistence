package de.invesdwin.context.persistence.leveldb.ipc;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.bean.tuple.Pair;

@Immutable
public final class SynchronousChannels {

    public static final ISynchronousReader CLOSED_READER = new ISynchronousReader() {

        @Override
        public void close() throws IOException {}

        @Override
        public void open() throws IOException {}

        @Override
        public Pair<Integer, byte[]> readMessage() {
            return null;
        }

        @Override
        public boolean hasNext() throws IOException {
            throw new EOFException();
        }
    };
    public static final ISynchronousWriter CLOSED_WRITER = new ISynchronousWriter() {
        @Override
        public void close() throws IOException {}

        @Override
        public void open() throws IOException {}

        @Override
        public void write(final int type, final byte[] message) {}
    };

    private SynchronousChannels() {}

    public static ISynchronousReader synchronize(final ISynchronousReader delegate) {
        return new ISynchronousReader() {

            @Override
            public synchronized void close() throws IOException {
                delegate.close();
            }

            @Override
            public synchronized void open() throws IOException {
                delegate.open();
            }

            @Override
            public synchronized Pair<Integer, byte[]> readMessage() {
                return delegate.readMessage();
            }

            @Override
            public synchronized boolean hasNext() throws IOException {
                return delegate.hasNext();
            }
        };
    }

    public static ISynchronousWriter synchronize(final ISynchronousWriter delegate) {
        return new ISynchronousWriter() {

            @Override
            public synchronized void close() throws IOException {
                delegate.close();
            }

            @Override
            public synchronized void open() throws IOException {
                delegate.open();
            }

            @Override
            public synchronized void write(final int type, final byte[] message) {
                delegate.write(type, message);
            }

        };
    }

}
