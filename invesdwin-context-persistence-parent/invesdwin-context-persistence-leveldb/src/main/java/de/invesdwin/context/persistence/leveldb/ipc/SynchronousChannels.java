package de.invesdwin.context.persistence.leveldb.ipc;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.stop.ProcessStopper;
import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.instrument.DynamicInstrumentationProperties;
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
    private static final File TMPFS_FOLDER = new File("/dev/shm");
    private static final File TMPFS_FOLDER_OR_FALLBACK;

    private static Boolean namedPipeSupportedCached;
    static {
        if (TMPFS_FOLDER.exists()) {
            TMPFS_FOLDER_OR_FALLBACK = DynamicInstrumentationProperties.newTempDirectory(TMPFS_FOLDER);
        } else {
            TMPFS_FOLDER_OR_FALLBACK = ContextProperties.TEMP_DIRECTORY;
        }
    }

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

    public static ISynchronousReader synchronize(final ISynchronousReader delegate, final Object lock) {
        return new ISynchronousReader() {

            @Override
            public void close() throws IOException {
                synchronized (lock) {
                    delegate.close();
                }
            }

            @Override
            public void open() throws IOException {
                synchronized (lock) {
                    delegate.open();
                }
            }

            @Override
            public Pair<Integer, byte[]> readMessage() throws IOException {
                synchronized (lock) {
                    return delegate.readMessage();
                }
            }

            @Override
            public boolean hasNext() throws IOException {
                synchronized (lock) {
                    return delegate.hasNext();
                }
            }
        };
    }

    public static ISynchronousWriter synchronize(final ISynchronousWriter delegate, final Object lock) {
        return new ISynchronousWriter() {

            @Override
            public void close() throws IOException {
                synchronized (lock) {
                    delegate.close();
                }
            }

            @Override
            public void open() throws IOException {
                synchronized (lock) {
                    delegate.open();
                }
            }

            @Override
            public void write(final int type, final byte[] message) throws IOException {
                synchronized (lock) {
                    delegate.write(type, message);
                }
            }

        };
    }

    public static File getTmpfsFolderOrFallback() {
        return TMPFS_FOLDER_OR_FALLBACK;
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
            execCommand("mkfifo", file.getAbsolutePath());

        } catch (final Exception e) {
            //mac os
            try {
                execCommand("mknod", file.getAbsolutePath());
            } catch (final Exception e2) {
                return false;
            }
        }
        return true;
    }

    private static void execCommand(final String... command) throws Exception {
        new ProcessExecutor().command(command)
                .destroyOnExit()
                .timeout(1, TimeUnit.MINUTES)
                .exitValueNormal()
                .redirectOutput(Slf4jStream.of(SynchronousChannels.class).asInfo())
                .stopper(new ProcessStopper() {
                    @Override
                    public void stop(final Process process) {
                        process.destroy();
                    }
                })
                .execute();
    }

}
