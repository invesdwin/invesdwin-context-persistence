package de.invesdwin.context.persistence.leveldb.ipc;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.leveldb.ipc.mapped.MappedSynchronousReader;
import de.invesdwin.context.persistence.leveldb.ipc.mapped.MappedSynchronousWriter;
import de.invesdwin.context.persistence.leveldb.ipc.pipe.PipeSynchronousReader;
import de.invesdwin.context.persistence.leveldb.ipc.pipe.PipeSynchronousWriter;
import de.invesdwin.context.persistence.leveldb.ipc.queue.QueueSynchronousReader;
import de.invesdwin.context.persistence.leveldb.ipc.queue.QueueSynchronousWriter;
import de.invesdwin.context.persistence.leveldb.ipc.queue.blocking.BlockingQueueSynchronousReader;
import de.invesdwin.context.persistence.leveldb.ipc.queue.blocking.BlockingQueueSynchronousWriter;
import de.invesdwin.context.persistence.leveldb.serde.FDateSerde;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.bean.tuple.Pair;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.error.UnknownArgumentException;
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
    private static final int VALUES = DEBUG ? 10 : 1_000_000;
    private static final int FLUSH_INTERVAL = Math.max(10, VALUES / 10);
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final Duration MAX_WAIT_DURATION = new Duration(1, DEBUG ? FTimeUnit.DAYS : FTimeUnit.SECONDS);

    private enum ChannelType {
        PIPE,
        MAPPED,
        QUEUE;
    }

    private File newFile(final String name, final boolean tmpfs, final ChannelType pipes) {
        final File baseFolder;
        if (tmpfs) {
            baseFolder = SynchronousChannels.TMPFS_FOLDER;
        } else {
            baseFolder = ContextProperties.TEMP_DIRECTORY;
        }
        final File file = new File(baseFolder, name);
        FileUtils.deleteQuietly(file);
        Assertions.checkFalse(file.exists(), "%s", file);
        if (pipes == ChannelType.PIPE) {
            Assertions.checkTrue(SynchronousChannels.createNamedPipe(file));
        } else if (pipes == ChannelType.MAPPED) {
            try {
                FileUtils.touch(file);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw UnknownArgumentException.newInstance(ChannelType.class, pipes);
        }
        Assertions.checkTrue(file.exists());
        return file;
    }

    @Test
    public void testNamedPipePerformance() throws InterruptedException {
        final boolean tmpfs = false;
        final ChannelType pipes = ChannelType.PIPE;
        final File requestFile = newFile("testNamedPipePerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testNamedPipePerformance_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null);
    }

    @Test
    public void testNamedPipePerformanceWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final ChannelType pipes = ChannelType.PIPE;
        final File requestFile = newFile("testNamedPipePerformanceWithTmpfs_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testNamedPipePerformanceWithTmpfs_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null);
    }

    @Test
    public void testMappedMemoryPerformance() throws InterruptedException {
        final boolean tmpfs = false;
        final ChannelType pipes = ChannelType.MAPPED;
        final File requestFile = newFile("testMappedMemoryPerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testMappedMemoryPerformance_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null);
    }

    @Test
    public void testMappedMemoryPerformanceWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final ChannelType pipes = ChannelType.MAPPED;
        final File requestFile = newFile("testMappedMemoryPerformanceWithTmpfs_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testMappedMemoryPerformanceWithTmpfs_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null);
    }

    @Test
    public void testArrayDequePerformance() throws InterruptedException {
        //ArrayDeque is not threadsafe, thus requires manual synchronization
        final Queue<Pair<Integer, byte[]>> responseQueue = new ArrayDeque<Pair<Integer, byte[]>>();
        final Queue<Pair<Integer, byte[]>> requestQueue = new ArrayDeque<Pair<Integer, byte[]>>();
        runQueuePerformanceTest(responseQueue, requestQueue, requestQueue, responseQueue);
    }

    @Test
    public void testLinkedBlockingQueuePerformance() throws InterruptedException {
        final Queue<Pair<Integer, byte[]>> responseQueue = new LinkedBlockingQueue<Pair<Integer, byte[]>>();
        final Queue<Pair<Integer, byte[]>> requestQueue = new LinkedBlockingQueue<Pair<Integer, byte[]>>();
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testLinkedBlockingQueuePerformanceWithBlocking() throws InterruptedException {
        final BlockingQueue<Pair<Integer, byte[]>> responseQueue = new LinkedBlockingQueue<Pair<Integer, byte[]>>();
        final BlockingQueue<Pair<Integer, byte[]>> requestQueue = new LinkedBlockingQueue<Pair<Integer, byte[]>>();
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testSynchronousQueuePerformance() throws InterruptedException {
        final SynchronousQueue<Pair<Integer, byte[]>> responseQueue = new SynchronousQueue<Pair<Integer, byte[]>>(
                false);
        final SynchronousQueue<Pair<Integer, byte[]>> requestQueue = new SynchronousQueue<Pair<Integer, byte[]>>(false);
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testSynchronousQueuePerformanceWithFair() throws InterruptedException {
        final SynchronousQueue<Pair<Integer, byte[]>> responseQueue = new SynchronousQueue<Pair<Integer, byte[]>>(true);
        final SynchronousQueue<Pair<Integer, byte[]>> requestQueue = new SynchronousQueue<Pair<Integer, byte[]>>(true);
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    private void runQueuePerformanceTest(final Queue<Pair<Integer, byte[]>> responseQueue,
            final Queue<Pair<Integer, byte[]>> requestQueue, final Object synchronizeRequest,
            final Object synchronizeResponse) throws InterruptedException {
        final ISynchronousWriter responseWriter = maybeSynchronize(new QueueSynchronousWriter(responseQueue),
                synchronizeResponse);
        final ISynchronousReader requestReader = maybeSynchronize(new QueueSynchronousReader(requestQueue),
                synchronizeRequest);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testQueuePerformance", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter requestWriter = maybeSynchronize(new QueueSynchronousWriter(requestQueue),
                synchronizeRequest);
        final ISynchronousReader responseReader = maybeSynchronize(new QueueSynchronousReader(responseQueue),
                synchronizeResponse);
        read(requestWriter, responseReader);
        executor.shutdown();
        executor.awaitTermination();
    }

    private void runBlockingQueuePerformanceTest(final BlockingQueue<Pair<Integer, byte[]>> responseQueue,
            final BlockingQueue<Pair<Integer, byte[]>> requestQueue, final Object synchronizeRequest,
            final Object synchronizeResponse) throws InterruptedException {
        final ISynchronousWriter responseWriter = maybeSynchronize(new BlockingQueueSynchronousWriter(responseQueue),
                synchronizeResponse);
        final ISynchronousReader requestReader = maybeSynchronize(new BlockingQueueSynchronousReader(requestQueue),
                synchronizeRequest);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testBlockingQueuePerformance", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter requestWriter = maybeSynchronize(new BlockingQueueSynchronousWriter(requestQueue),
                synchronizeRequest);
        final ISynchronousReader responseReader = maybeSynchronize(new BlockingQueueSynchronousReader(responseQueue),
                synchronizeResponse);
        read(requestWriter, responseReader);
        executor.shutdown();
        executor.awaitTermination();
    }

    private ISynchronousReader maybeSynchronize(final ISynchronousReader reader, final Object synchronize) {
        if (synchronize != null) {
            return SynchronousChannels.synchronize(reader, synchronize);
        } else {
            return reader;
        }
    }

    private ISynchronousWriter maybeSynchronize(final ISynchronousWriter writer, final Object synchronize) {
        if (synchronize != null) {
            return SynchronousChannels.synchronize(writer, synchronize);
        } else {
            return writer;
        }
    }

    private void runPerformanceTest(final ChannelType pipes, final File requestFile, final File responseFile,
            final Object synchronizeRequest, final Object synchronizeResponse) throws InterruptedException {
        try {
            final ISynchronousWriter responseWriter = maybeSynchronize(newWriter(responseFile, pipes),
                    synchronizeResponse);
            final ISynchronousReader requestReader = maybeSynchronize(newReader(requestFile, pipes),
                    synchronizeRequest);
            final WrappedExecutorService executor = Executors.newFixedThreadPool(responseFile.getName(), 1);
            executor.execute(new WriterTask(requestReader, responseWriter));
            final ISynchronousWriter requestWriter = maybeSynchronize(newWriter(requestFile, pipes),
                    synchronizeRequest);
            final ISynchronousReader responseReader = maybeSynchronize(newReader(responseFile, pipes),
                    synchronizeResponse);
            read(requestWriter, responseReader);
            executor.shutdown();
            executor.awaitTermination();
        } finally {
            FileUtils.deleteQuietly(requestFile);
            FileUtils.deleteQuietly(responseFile);
        }
    }

    private ISynchronousReader newReader(final File file, final ChannelType pipes) {
        if (pipes == ChannelType.PIPE) {
            return new PipeSynchronousReader(file, MESSAGE_SIZE);
        } else if (pipes == ChannelType.MAPPED) {
            return new MappedSynchronousReader(file, MESSAGE_SIZE);
        } else {
            throw UnknownArgumentException.newInstance(ChannelType.class, pipes);
        }
    }

    private ISynchronousWriter newWriter(final File file, final ChannelType pipes) {
        if (pipes == ChannelType.PIPE) {
            return new PipeSynchronousWriter(file, MESSAGE_SIZE);
        } else if (pipes == ChannelType.MAPPED) {
            return new MappedSynchronousWriter(file, MESSAGE_SIZE);
        } else {
            throw UnknownArgumentException.newInstance(ChannelType.class, pipes);
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
                final Pair<Integer, byte[]> readMessage = responseReader.readMessage();
                if (DEBUG) {
                    log.info("client response in");
                }
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
            if (DEBUG) {
                log.info("client close reader");
            }
            responseReader.close();
            if (DEBUG) {
                log.info("client close writer");
            }
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
                if (DEBUG) {
                    log.info("server close writer");
                }
                responseWriter.close();
                if (DEBUG) {
                    log.info("server close reader");
                }
                requestReader.close();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

}
