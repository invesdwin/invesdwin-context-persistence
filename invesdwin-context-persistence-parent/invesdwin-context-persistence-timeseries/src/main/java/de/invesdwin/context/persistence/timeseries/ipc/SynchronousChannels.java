package de.invesdwin.context.persistence.timeseries.ipc;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang3.BooleanUtils;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.stop.ProcessStopper;
import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseries.ipc.response.ISynchronousResponse;
import de.invesdwin.instrument.DynamicInstrumentationProperties;
import de.invesdwin.util.lang.Files;

@Immutable
public final class SynchronousChannels {

    private static final File TMPFS_FOLDER = new File("/dev/shm");
    @GuardedBy("SynchronousChannels.class")
    private static File tmpfsFolderOrFallback;
    @GuardedBy("SynchronousChannels.class")
    private static Boolean namedPipeSupportedCached;

    private SynchronousChannels() {
    }

    public static <T> ISynchronousReader<T> synchronize(final ISynchronousReader<T> delegate) {
        return new ISynchronousReader<T>() {

            @Override
            public synchronized void close() throws IOException {
                delegate.close();
            }

            @Override
            public synchronized void open() throws IOException {
                delegate.open();
            }

            @Override
            public synchronized ISynchronousResponse<T> readMessage() throws IOException {
                return delegate.readMessage();
            }

            @Override
            public synchronized boolean hasNext() throws IOException {
                return delegate.hasNext();
            }
        };
    }

    public static <T> ISynchronousWriter<T> synchronize(final ISynchronousWriter<T> delegate) {
        return new ISynchronousWriter<T>() {

            @Override
            public synchronized void close() throws IOException {
                delegate.close();
            }

            @Override
            public synchronized void open() throws IOException {
                delegate.open();
            }

            @Override
            public synchronized void write(final int type, final int sequence, final T message) throws IOException {
                delegate.write(type, sequence, message);
            }

            @Override
            public synchronized void write(final ISynchronousResponse<T> response) throws IOException {
                delegate.write(response);
            }

        };
    }

    public static <T> ISynchronousReader<T> synchronize(final ISynchronousReader<T> delegate, final Object lock) {
        return new ISynchronousReader<T>() {

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
            public ISynchronousResponse<T> readMessage() throws IOException {
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

    public static <T> ISynchronousWriter<T> synchronize(final ISynchronousWriter<T> delegate, final Object lock) {
        return new ISynchronousWriter<T>() {

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
            public void write(final int type, final int sequence, final T message) throws IOException {
                synchronized (lock) {
                    delegate.write(type, sequence, message);
                }
            }

            @Override
            public void write(final ISynchronousResponse<T> response) throws IOException {
                synchronized (lock) {
                    delegate.write(response);
                }
            }

        };
    }

    public static synchronized File getTmpfsFolderOrFallback() {
        if (tmpfsFolderOrFallback == null) {
            if (TMPFS_FOLDER.exists()) {
                tmpfsFolderOrFallback = DynamicInstrumentationProperties.newTempDirectory(TMPFS_FOLDER);
            } else {
                tmpfsFolderOrFallback = ContextProperties.TEMP_DIRECTORY;
            }
        }
        return tmpfsFolderOrFallback;
    }

    public static synchronized boolean isNamedPipeSupported() {
        if (namedPipeSupportedCached == null) {
            final File namedPipeTestFile = new File(ContextProperties.TEMP_DIRECTORY,
                    SynchronousChannels.class.getSimpleName() + "_NamedPipeTest.pipe");
            namedPipeSupportedCached = namedPipeTestFile.exists() || createNamedPipe(namedPipeTestFile);
            Files.deleteQuietly(namedPipeTestFile);
        }
        return namedPipeSupportedCached;
    }

    public static synchronized boolean createNamedPipe(final File file) {
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
                .redirectError(Slf4jStream.of(SynchronousChannels.class).asWarn())
                .stopper(new ProcessStopper() {
                    @Override
                    public void stop(final Process process) {
                        process.destroy();
                    }
                })
                .execute();
    }

}
