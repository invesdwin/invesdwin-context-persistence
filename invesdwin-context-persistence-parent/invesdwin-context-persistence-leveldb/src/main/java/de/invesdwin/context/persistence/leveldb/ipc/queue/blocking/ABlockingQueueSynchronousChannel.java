package de.invesdwin.context.persistence.leveldb.ipc.queue.blocking;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.math3.random.JDKRandomGenerator;

import de.invesdwin.context.persistence.leveldb.ipc.ISynchronousChannel;
import de.invesdwin.context.persistence.leveldb.ipc.queue.QueueSynchronousWriter;
import de.invesdwin.util.bean.tuple.Pair;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;

@NotThreadSafe
public abstract class ABlockingQueueSynchronousChannel implements ISynchronousChannel {

    public static final WrappedExecutorService CLOSED_SYNCHRONIZER = Executors
            .newCachedThreadPool(ABlockingQueueSynchronousChannel.class.getSimpleName() + "_closedSynchronizer");

    protected BlockingQueue<Pair<Integer, byte[]>> queue;

    public ABlockingQueueSynchronousChannel(final BlockingQueue<Pair<Integer, byte[]>> queue) {
        this.queue = queue;
    }

    @Override
    public void open() throws IOException {}

    @Override
    public void close() throws IOException {
        if (queue != null) {
            sendClosedMessage();
            queue = null;
        }
    }

    protected void sendClosedMessage() {
        final BlockingQueue<Pair<Integer, byte[]>> queueCopy = queue;
        CLOSED_SYNCHRONIZER.execute(new Runnable() {
            @Override
            public void run() {
                //randomize sleeps to increase chance of meeting each other
                final JDKRandomGenerator random = new JDKRandomGenerator();
                try {
                    boolean closedMessageSent = false;
                    boolean closedMessageReceived = false;
                    while (!closedMessageReceived || !closedMessageSent) {
                        if (queueCopy.poll(random.nextInt(2),
                                TimeUnit.MILLISECONDS) == QueueSynchronousWriter.CLOSED_MESSAGE) {
                            closedMessageReceived = true;
                        }
                        if (queueCopy.offer(QueueSynchronousWriter.CLOSED_MESSAGE, random.nextInt(2),
                                TimeUnit.MILLISECONDS)) {
                            closedMessageSent = true;
                        }
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

    }

}
