package de.invesdwin.context.persistence.leveldb.ipc;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.BooleanUtils;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.util.bean.tuple.Pair;

@Immutable
public final class SynchronousChannels {

    public static final File SHM_FOLDER = new File("/dev/shm");

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

    private static Boolean namedPipeSupportedCached;

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
            public synchronized Pair<Integer, byte[]> readMessage() throws IOException {
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
            public synchronized void write(final int type, final byte[] message) throws IOException {
                delegate.write(type, message);
            }

        };
    }

    public static File getSharedMemoryFolderOrFallback() {
        if (SHM_FOLDER.exists()) {
            return SHM_FOLDER;
        } else {
            return ContextProperties.TEMP_DIRECTORY;
        }
    }

    public static boolean isNamedPipeSupported() {
        if (namedPipeSupportedCached == null) {
            final File namedPipeTestFile = new File(ContextProperties.TEMP_DIRECTORY,
                    SynchronousChannels.class.getSimpleName() + "_NamedPipeTest.pipe");
            namedPipeSupportedCached = namedPipeTestFile.exists() || createNamedPipe(namedPipeTestFile);
            FileUtils.deleteQuietly(namedPipeTestFile);
        }
        return namedPipeSupportedCached;
    }

    public static boolean createNamedPipe(final File file) {
        if (BooleanUtils.isFalse(namedPipeSupportedCached)) {
            return false;
        }
        try {
            //linux
            Runtime.getRuntime().exec("mkfifo " + file.getAbsolutePath());
        } catch (final IOException e) {
            //mac os
            try {
                Runtime.getRuntime().exec("mknod " + file.getAbsolutePath());
            } catch (final IOException e2) {
                return false;
            }
        }
        return true;
    }

}
