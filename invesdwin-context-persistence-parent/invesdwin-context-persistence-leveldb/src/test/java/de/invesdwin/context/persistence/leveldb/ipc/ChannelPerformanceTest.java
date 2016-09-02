package de.invesdwin.context.persistence.leveldb.ipc;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.leveldb.ipc.mapped.MappedSynchronousReader;
import de.invesdwin.context.persistence.leveldb.ipc.mapped.MappedSynchronousWriter;
import de.invesdwin.context.persistence.leveldb.ipc.pipe.PipeSynchronousReader;
import de.invesdwin.context.persistence.leveldb.ipc.pipe.PipeSynchronousWriter;
import de.invesdwin.context.persistence.leveldb.serde.FDateSerde;
import de.invesdwin.context.persistence.leveldb.timeseries.ATimeSeriesUpdater;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.bean.tuple.Pair;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.math.decimal.Decimal;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.math.decimal.scaled.PercentScale;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FTimeUnit;

@NotThreadSafe
public class ChannelPerformanceTest extends ATest {

    private static final boolean DEBUG = false;
    private static final int MESSAGE_SIZE = FDateSerde.get.toBytes(FDate.MAX_DATE).length;
    private static final int MESSAGE_TYPE = 1;
    private static final int VALUES = DEBUG ? 10 : 100_000;
    private static final int FLUSH_INTERVAL = ATimeSeriesUpdater.BATCH_FLUSH_INTERVAL;
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final Duration MAX_WAIT_DURATION = new Duration(10, FTimeUnit.SECONDS);

    private File newFile(final String name, final boolean tmpfs, final boolean pipes) {
        final File baseFolder;
        if (tmpfs) {
            baseFolder = SynchronousChannels.TMPFS_FOLDER;
        } else {
            baseFolder = ContextProperties.TEMP_DIRECTORY;
        }
        final File file = new File(baseFolder, name);
        FileUtils.deleteQuietly(file);
        Assertions.checkFalse(file.exists(), "%s", file);
        if (pipes) {
            Assertions.checkTrue(SynchronousChannels.createNamedPipe(file));
        } else {
            try {
                FileUtils.touch(file);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
        Assertions.checkTrue(file.exists());
        return file;
    }

    @Test
    public void testNamedPipePerformance() throws InterruptedException {
        final boolean tmpfs = false;
        final boolean pipes = true;
        final File requestFile = newFile("testNamedPipePerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testNamedPipePerformance_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile);
    }

    @Test
    public void testNamedPipePerformanceWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final boolean pipes = true;
        final File requestFile = newFile("testNamedPipePerformanceWithTmpfs_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testNamedPipePerformanceWithTmpfs_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile);
    }

    @Test
    public void testMappedMemoryPerformance() throws InterruptedException {
        final boolean tmpfs = false;
        final boolean pipes = false;
        final File requestFile = newFile("testMappedMemoryPerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testMappedMemoryPerformance_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile);
    }

    @Test
    public void testMappedMemoryPerformanceWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final boolean pipes = false;
        final File requestFile = newFile("testMappedMemoryPerformanceWithTmpfs_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testMappedMemoryPerformanceWithTmpfs_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile);
    }

    private void runPerformanceTest(final boolean pipes, final File requestFile, final File responseFile)
            throws InterruptedException {
        try {
            final ISynchronousWriter responseWriter = newWriter(responseFile, pipes);
            final ISynchronousReader requestReader = newReader(requestFile, pipes);
            final WrappedExecutorService executor = Executors.newFixedThreadPool(responseFile.getName(), 1);
            executor.execute(new WriterTask(requestReader, responseWriter));
            final ISynchronousWriter requestWriter = newWriter(requestFile, pipes);
            final ISynchronousReader responseReader = newReader(responseFile, pipes);
            read(requestWriter, responseReader);
            executor.shutdown();
            executor.awaitTermination();
        } finally {
            FileUtils.deleteQuietly(requestFile);
            FileUtils.deleteQuietly(responseFile);
        }
    }

    private ISynchronousReader newReader(final File file, final boolean pipes) {
        if (pipes) {
            return new PipeSynchronousReader(file, MESSAGE_SIZE);
        } else {
            return new MappedSynchronousReader(file, MESSAGE_SIZE);
        }
    }

    private ISynchronousWriter newWriter(final File file, final boolean pipes) {
        if (pipes) {
            return new PipeSynchronousWriter(file, MESSAGE_SIZE);
        } else {
            return new MappedSynchronousWriter(file, MESSAGE_SIZE);
        }
    }

    private void read(final ISynchronousWriter requestWriter, final ISynchronousReader responseReader) {
        final Instant readsStart = new Instant();
        FDate prevValue = null;
        int count = 0;
        try {
            requestWriter.open();
            responseReader.open();
            final ASpinWait spinWait = new ASpinWait() {
                @Override
                protected boolean isConditionFulfilled() throws IOException {
                    return responseReader.hasNext();
                }
            };
            Instant waitingSince = new Instant();
            while (true) {
                requestWriter.write(MESSAGE_TYPE, EMPTY_BYTES);
                if (DEBUG) {
                    log.info("client request out");
                }
                Assertions.checkTrue(spinWait.awaitFulfill(waitingSince, MAX_WAIT_DURATION));
                if (DEBUG) {
                    log.info("client response in");
                }
                final Pair<Integer, byte[]> readMessage = responseReader.readMessage();
                final int messageType = readMessage.getFirst();
                final byte[] responseBytes = readMessage.getSecond();
                Assertions.checkEquals(messageType, MESSAGE_TYPE);
                Assertions.checkEquals(responseBytes.length, MESSAGE_SIZE);
                final FDate value = FDateSerde.get.fromBytes(responseBytes);
                if (prevValue != null) {
                    Assertions.checkTrue(prevValue.isBefore(value));
                }
                prevValue = value;
                count++;
                waitingSince = new Instant();
            }
        } catch (final EOFException e) {
            //writer closed
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        Assertions.checkEquals(count, VALUES);
        try {
            responseReader.close();
            requestWriter.close();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        printProgress("ReadsFinished", readsStart, VALUES, VALUES);
    }

    private void printProgress(final String action, final Instant start, final int count, final int maxCount) {
        final long milliseconds = start.toDuration().longValue(FTimeUnit.MILLISECONDS);
        log.info("%s: %s/%s (%s) %s/ms in %s ms", action, count, maxCount,
                new Percent(count, maxCount).toString(PercentScale.PERCENT),
                Decimal.valueOf(count).divide(milliseconds).round(2), milliseconds);
    }

    private ICloseableIterable<FDate> newValues() {
        return FDate.iterable(FDate.MIN_DATE, FDate.MIN_DATE.addMilliseconds(VALUES - 1), FTimeUnit.MILLISECONDS, 1);
    }

    private class WriterTask implements Runnable {

        private final ISynchronousReader requestReader;
        private final ISynchronousWriter responseWriter;

        WriterTask(final ISynchronousReader requestReader, final ISynchronousWriter responseWriter) {
            this.requestReader = requestReader;
            this.responseWriter = responseWriter;
        }

        @Override
        public void run() {
            final ASpinWait spinWait = new ASpinWait() {
                @Override
                protected boolean isConditionFulfilled() throws IOException {
                    return requestReader.hasNext();
                }
            };
            try {
                final Instant writesStart = new Instant();
                int i = 0;
                requestReader.open();
                responseWriter.open();
                Instant waitingSince = new Instant();
                for (final FDate date : newValues()) {
                    Assertions.checkTrue(spinWait.awaitFulfill(waitingSince, MAX_WAIT_DURATION));
                    if (DEBUG) {
                        log.info("server request in");
                    }
                    final Pair<Integer, byte[]> readMessage = requestReader.readMessage();
                    Assertions.checkEquals(readMessage.getFirst(), MESSAGE_TYPE);
                    Assertions.checkEquals(readMessage.getSecond().length, 0);
                    final byte[] responseBytes = FDateSerde.get.toBytes(date);
                    Assertions.checkEquals(responseBytes.length, MESSAGE_SIZE);
                    responseWriter.write(MESSAGE_TYPE, responseBytes);
                    if (DEBUG) {
                        log.info("server response out");
                    }
                    i++;
                    if (i % FLUSH_INTERVAL == 0) {
                        printProgress("Writes", writesStart, i, VALUES);
                    }
                    waitingSince = new Instant();
                }
                printProgress("WritesFinished", writesStart, VALUES, VALUES);
                responseWriter.close();
                requestReader.close();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

}
