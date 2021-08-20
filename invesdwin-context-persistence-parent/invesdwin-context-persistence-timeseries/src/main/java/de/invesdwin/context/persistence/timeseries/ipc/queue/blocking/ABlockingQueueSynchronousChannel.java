package de.invesdwin.context.persistence.timeseries.ipc.queue.blocking;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.math3.random.RandomGenerator;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousChannel;
import de.invesdwin.context.persistence.timeseries.ipc.response.ClosedSynchronousResponse;
import de.invesdwin.context.persistence.timeseries.ipc.response.ISynchronousResponse;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.math.random.RandomGenerators;

@NotThreadSafe
public abstract class ABlockingQueueSynchronousChannel<M> implements ISynchronousChannel {

    public static final WrappedExecutorService CLOSED_SYNCHRONIZER = Executors
            .newCachedThreadPool(ABlockingQueueSynchronousChannel.class.getSimpleName() + "_closedSynchronizer");

    protected BlockingQueue<ISynchronousResponse<M>> queue;

    public ABlockingQueueSynchronousChannel(final BlockingQueue<ISynchronousResponse<M>> queue) {
        this.queue = queue;
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public void close() throws IOException {
        if (queue != null) {
            sendClosedMessage();
            queue = null;
        }
    }

    protected void sendClosedMessage() {
        final BlockingQueue<ISynchronousResponse<M>> queueCopy = queue;
        CLOSED_SYNCHRONIZER.execute(new Runnable() {
            @Override
            public void run() {
                //randomize sleeps to increase chance of meeting each other
                final RandomGenerator random = RandomGenerators.currentThreadLocalRandom();
                try {
                    boolean closedMessageSent = false;
                    boolean closedMessageReceived = false;
                    while (!closedMessageReceived || !closedMessageSent) {
                        if (queueCopy.poll(random.nextInt(2), TimeUnit.MILLISECONDS) == ClosedSynchronousResponse
                                .getInstance()) {
                            closedMessageReceived = true;
                        }
                        if (queueCopy.offer(ClosedSynchronousResponse.getInstance(), random.nextInt(2),
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
