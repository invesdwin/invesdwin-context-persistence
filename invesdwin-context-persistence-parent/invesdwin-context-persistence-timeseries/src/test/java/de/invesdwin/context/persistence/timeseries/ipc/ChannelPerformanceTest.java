package de.invesdwin.context.persistence.timeseries.ipc;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.function.BooleanSupplier;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Ignore;
import org.junit.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseries.ipc.mapped.MappedSynchronousReader;
import de.invesdwin.context.persistence.timeseries.ipc.mapped.MappedSynchronousWriter;
import de.invesdwin.context.persistence.timeseries.ipc.pipe.PipeSynchronousReader;
import de.invesdwin.context.persistence.timeseries.ipc.pipe.PipeSynchronousWriter;
import de.invesdwin.context.persistence.timeseries.ipc.queue.QueueSynchronousReader;
import de.invesdwin.context.persistence.timeseries.ipc.queue.QueueSynchronousWriter;
import de.invesdwin.context.persistence.timeseries.ipc.queue.blocking.BlockingQueueSynchronousReader;
import de.invesdwin.context.persistence.timeseries.ipc.queue.blocking.BlockingQueueSynchronousWriter;
import de.invesdwin.context.persistence.timeseries.ipc.socket.SocketSynchronousReader;
import de.invesdwin.context.persistence.timeseries.ipc.socket.SocketSynchronousWriter;
import de.invesdwin.context.persistence.timeseries.ipc.socket.udp.DatagramSocketSynchronousReader;
import de.invesdwin.context.persistence.timeseries.ipc.socket.udp.DatagramSocketSynchronousWriter;
import de.invesdwin.context.persistence.timeseries.serde.FDateSerde;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.concurrent.ASpinWait;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.ProcessedEventsRateString;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.math.decimal.scaled.PercentScale;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FDates;
import de.invesdwin.util.time.fdate.FTimeUnit;

// CHECKSTYLE:OFF
@NotThreadSafe
@Ignore("manual test")
public class ChannelPerformanceTest extends ATest {
    //CHECKSTYLE:ON

    private static final boolean DEBUG = false;
    private static final int MESSAGE_SIZE = FDateSerde.FIXED_LENGTH;
    private static final int MESSAGE_TYPE = 1;
    private static final int MESSAGE_SEQUENCE = 1;
    private static final int VALUES = DEBUG ? 10 : 1_000_000;
    private static final int FLUSH_INTERVAL = Math.max(10, VALUES / 10);
    private static final Duration MAX_WAIT_DURATION = new Duration(10, DEBUG ? FTimeUnit.DAYS : FTimeUnit.SECONDS);

    private enum FileChannelType {
        PIPE,
        MAPPED;
    }

    private File newFile(final String name, final boolean tmpfs, final FileChannelType pipes) {
        final File baseFolder;
        if (tmpfs) {
            baseFolder = SynchronousChannels.getTmpfsFolderOrFallback();
        } else {
            baseFolder = ContextProperties.TEMP_DIRECTORY;
        }
        final File file = new File(baseFolder, name);
        Files.deleteQuietly(file);
        Assertions.checkFalse(file.exists(), "%s", file);
        if (pipes == FileChannelType.PIPE) {
            Assertions.checkTrue(SynchronousChannels.createNamedPipe(file));
        } else if (pipes == FileChannelType.MAPPED) {
            try {
                Files.touch(file);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw UnknownArgumentException.newInstance(FileChannelType.class, pipes);
        }
        Assertions.checkTrue(file.exists());
        return file;
    }

    @Test
    public void testNamedPipePerformance() throws InterruptedException {
        final boolean tmpfs = false;
        final FileChannelType pipes = FileChannelType.PIPE;
        final File requestFile = newFile("testNamedPipePerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testNamedPipePerformance_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null);
    }

    @Test
    public void testNamedPipePerformanceWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.PIPE;
        final File requestFile = newFile("testNamedPipePerformanceWithTmpfs_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testNamedPipePerformanceWithTmpfs_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null);
    }

    @Test
    public void testMappedMemoryPerformance() throws InterruptedException {
        final boolean tmpfs = false;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testMappedMemoryPerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testMappedMemoryPerformance_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null);
    }

    @Test
    public void testMappedMemoryPerformanceWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testMappedMemoryPerformanceWithTmpfs_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testMappedMemoryPerformanceWithTmpfs_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null);
    }

    @Test
    public void testArrayDequePerformance() throws InterruptedException {
        //ArrayDeque is not threadsafe, thus requires manual synchronization
        final Queue<SynchronousResponse> responseQueue = new ArrayDeque<SynchronousResponse>();
        final Queue<SynchronousResponse> requestQueue = new ArrayDeque<SynchronousResponse>();
        runQueuePerformanceTest(responseQueue, requestQueue, requestQueue, responseQueue);
    }

    @Test
    public void testLinkedBlockingQueuePerformance() throws InterruptedException {
        final Queue<SynchronousResponse> responseQueue = new LinkedBlockingQueue<SynchronousResponse>();
        final Queue<SynchronousResponse> requestQueue = new LinkedBlockingQueue<SynchronousResponse>();
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testLinkedBlockingQueuePerformanceWithBlocking() throws InterruptedException {
        final BlockingQueue<SynchronousResponse> responseQueue = new LinkedBlockingQueue<SynchronousResponse>();
        final BlockingQueue<SynchronousResponse> requestQueue = new LinkedBlockingQueue<SynchronousResponse>();
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testSynchronousQueuePerformance() throws InterruptedException {
        final SynchronousQueue<SynchronousResponse> responseQueue = new SynchronousQueue<SynchronousResponse>(false);
        final SynchronousQueue<SynchronousResponse> requestQueue = new SynchronousQueue<SynchronousResponse>(false);
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testSynchronousQueuePerformanceWithFair() throws InterruptedException {
        final SynchronousQueue<SynchronousResponse> responseQueue = new SynchronousQueue<SynchronousResponse>(true);
        final SynchronousQueue<SynchronousResponse> requestQueue = new SynchronousQueue<SynchronousResponse>(true);
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    private void runQueuePerformanceTest(final Queue<SynchronousResponse> responseQueue,
            final Queue<SynchronousResponse> requestQueue, final Object synchronizeRequest,
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

    private void runBlockingQueuePerformanceTest(final BlockingQueue<SynchronousResponse> responseQueue,
            final BlockingQueue<SynchronousResponse> requestQueue, final Object synchronizeRequest,
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

    @Test
    public void testSocketPerformance() throws InterruptedException {
        final SocketAddress responseAddress = new InetSocketAddress("localhost", 7878);
        final SocketAddress requestAddress = new InetSocketAddress("localhost", 7879);
        runSocketPerformanceTest(responseAddress, requestAddress);
    }

    private void runSocketPerformanceTest(final SocketAddress responseAddress, final SocketAddress requestAddress)
            throws InterruptedException {
        final ISynchronousWriter responseWriter = new SocketSynchronousWriter(responseAddress, true, MESSAGE_SIZE);
        final ISynchronousReader requestReader = new SocketSynchronousReader(requestAddress, true, MESSAGE_SIZE);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testSocketPerformance", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter requestWriter = new SocketSynchronousWriter(requestAddress, false, MESSAGE_SIZE);
        final ISynchronousReader responseReader = new SocketSynchronousReader(responseAddress, false, MESSAGE_SIZE);
        read(requestWriter, responseReader);
        executor.shutdown();
        executor.awaitTermination();
    }

    @Test
    public void testDatagramSocketPerformance() throws InterruptedException {
        final SocketAddress responseAddress = new InetSocketAddress("localhost", 7878);
        final SocketAddress requestAddress = new InetSocketAddress("localhost", 7879);
        runDatagramSocketPerformanceTest(responseAddress, requestAddress);
    }

    private void runDatagramSocketPerformanceTest(final SocketAddress responseAddress,
            final SocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter responseWriter = new DatagramSocketSynchronousWriter(responseAddress, MESSAGE_SIZE);
        final ISynchronousReader requestReader = new DatagramSocketSynchronousReader(requestAddress, MESSAGE_SIZE);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testDatagramSocketPerformance", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter requestWriter = new DatagramSocketSynchronousWriter(requestAddress, MESSAGE_SIZE);
        final ISynchronousReader responseReader = new DatagramSocketSynchronousReader(responseAddress, MESSAGE_SIZE);
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

    private void runPerformanceTest(final FileChannelType pipes, final File requestFile, final File responseFile,
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
            Files.deleteQuietly(requestFile);
            Files.deleteQuietly(responseFile);
        }
    }

    private ISynchronousReader newReader(final File file, final FileChannelType pipes) {
        if (pipes == FileChannelType.PIPE) {
            return new PipeSynchronousReader(file, MESSAGE_SIZE);
        } else if (pipes == FileChannelType.MAPPED) {
            return new MappedSynchronousReader(file, MESSAGE_SIZE);
        } else {
            throw UnknownArgumentException.newInstance(FileChannelType.class, pipes);
        }
    }

    private ISynchronousWriter newWriter(final File file, final FileChannelType pipes) {
        if (pipes == FileChannelType.PIPE) {
            return new PipeSynchronousWriter(file, MESSAGE_SIZE);
        } else if (pipes == FileChannelType.MAPPED) {
            return new MappedSynchronousWriter(file, MESSAGE_SIZE);
        } else {
            throw UnknownArgumentException.newInstance(FileChannelType.class, pipes);
        }
    }

    private void read(final ISynchronousWriter requestWriter, final ISynchronousReader responseReader) {
        final Instant readsStart = new Instant();
        FDate prevValue = null;
        int count = 0;
        try {
            if (DEBUG) {
                log.info("client open request writer");
            }
            requestWriter.open();
            if (DEBUG) {
                log.info("client open response reader");
            }
            responseReader.open();
            final ASpinWait spinWait = new ASpinWait() {
                @Override
                protected boolean isConditionFulfilled(final BooleanSupplier outerCondition) throws Exception {
                    return responseReader.hasNext();
                }
            };
            Instant waitingSince = new Instant();
            while (true) {
                requestWriter.write(MESSAGE_TYPE, MESSAGE_SEQUENCE, Bytes.EMPTY_ARRAY);
                if (DEBUG) {
                    log.info("client request out");
                }
                Assertions.checkTrue(spinWait.awaitFulfill(waitingSince, MAX_WAIT_DURATION));
                final SynchronousResponse readMessage = responseReader.readMessage();
                if (DEBUG) {
                    log.info("client response in");
                }
                final int messageType = readMessage.getType();
                final int messageSequence = readMessage.getSequence();
                final byte[] responseBytes = readMessage.getMessage();
                Assertions.checkEquals(messageType, MESSAGE_TYPE);
                Assertions.checkEquals(messageSequence, MESSAGE_SEQUENCE);
                Assertions.checkEquals(responseBytes.length, MESSAGE_SIZE);
                final FDate value = FDateSerde.GET.fromBytes(responseBytes);
                if (prevValue != null) {
                    Assertions.checkTrue(prevValue.isBefore(value));
                }
                prevValue = value;
                count++;
                waitingSince = new Instant();
            }
        } catch (final EOFException e) {
            //writer closed
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        Assertions.checkEquals(count, VALUES);
        try {
            if (DEBUG) {
                log.info("client close response reader");
            }
            responseReader.close();
            if (DEBUG) {
                log.info("client close request writer");
            }
            requestWriter.close();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        printProgress("ReadsFinished", readsStart, VALUES, VALUES);
    }

    private void printProgress(final String action, final Instant start, final int count, final int maxCount) {
        final Duration duration = start.toDuration();
        log.info("%s: %s/%s (%s) %s during %s", action, count, maxCount,
                new Percent(count, maxCount).toString(PercentScale.PERCENT),
                new ProcessedEventsRateString(count, duration), duration);
    }

    private ICloseableIterable<FDate> newValues() {
        return FDates.iterable(FDate.MIN_DATE, FDate.MIN_DATE.addMilliseconds(VALUES - 1), FTimeUnit.MILLISECONDS, 1);
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
                protected boolean isConditionFulfilled(final BooleanSupplier outerCondition) throws Exception {
                    return requestReader.hasNext();
                }
            };
            try {
                final Instant writesStart = new Instant();
                int i = 0;
                if (DEBUG) {
                    log.info("server open request reader");
                }
                requestReader.open();
                if (DEBUG) {
                    log.info("server open response writer");
                }
                responseWriter.open();
                Instant waitingSince = new Instant();
                for (final FDate date : newValues()) {
                    Assertions.checkTrue(spinWait.awaitFulfill(waitingSince, MAX_WAIT_DURATION));
                    if (DEBUG) {
                        log.info("server request in");
                    }
                    final SynchronousResponse readMessage = requestReader.readMessage();
                    Assertions.checkEquals(readMessage.getType(), MESSAGE_TYPE);
                    Assertions.checkEquals(readMessage.getSequence(), MESSAGE_SEQUENCE);
                    Assertions.checkEquals(readMessage.getMessage().length, 0);
                    final byte[] responseBytes = FDateSerde.GET.toBytes(date);
                    Assertions.checkEquals(responseBytes.length, MESSAGE_SIZE);
                    responseWriter.write(MESSAGE_TYPE, MESSAGE_SEQUENCE, responseBytes);
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
                    log.info("server close response writer");
                }
                responseWriter.close();
                if (DEBUG) {
                    log.info("server close request reader");
                }
                requestReader.close();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

}
