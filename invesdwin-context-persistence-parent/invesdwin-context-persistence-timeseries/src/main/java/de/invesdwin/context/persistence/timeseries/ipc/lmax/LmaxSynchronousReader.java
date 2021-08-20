package de.invesdwin.context.persistence.timeseries.ipc.lmax;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.lmax.disruptor.EventPoller;
import com.lmax.disruptor.RingBuffer;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousReader;
import de.invesdwin.context.persistence.timeseries.ipc.message.EmptySynchronousMessage;
import de.invesdwin.context.persistence.timeseries.ipc.message.ISynchronousMessage;
import de.invesdwin.context.persistence.timeseries.ipc.message.ImmutableSynchronousMessage;
import de.invesdwin.context.persistence.timeseries.ipc.message.MutableSynchronousMessage;

@NotThreadSafe
public class LmaxSynchronousReader<M> implements ISynchronousReader<M> {

    private RingBuffer<MutableSynchronousMessage<M>> ringBuffer;
    private EventPoller<MutableSynchronousMessage<M>> eventPoller;
    private ImmutableSynchronousMessage<M> polledValue;

    private final EventPoller.Handler<MutableSynchronousMessage<M>> pollerHandler = (event, sequence, endOfBatch) -> {
        polledValue = ImmutableSynchronousMessage.valueOf(event);
        return false;
    };

    public LmaxSynchronousReader(final RingBuffer<MutableSynchronousMessage<M>> ringBuffer) {
        this.ringBuffer = ringBuffer;
        this.eventPoller = ringBuffer.newPoller();
        ringBuffer.addGatingSequences(eventPoller.getSequence());
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public void close() throws IOException {
        ringBuffer = null;
        eventPoller = null;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (polledValue != null) {
            return true;
        }
        try {
            eventPoller.poll(pollerHandler);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        return polledValue != null;
    }

    @Override
    public ISynchronousMessage<M> readMessage() throws IOException {
        final ISynchronousMessage<M> message = getPolledMessage();
        if (message.getType() == EmptySynchronousMessage.TYPE) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

    private ISynchronousMessage<M> getPolledMessage() {
        if (polledValue != null) {
            final ImmutableSynchronousMessage<M> value = polledValue;
            polledValue = null;
            return value;
        }
        try {
            final EventPoller.PollState poll = eventPoller.poll(pollerHandler);
            if (poll == EventPoller.PollState.PROCESSING) {
                final ImmutableSynchronousMessage<M> value = polledValue;
                polledValue = null;
                return value;
            } else {
                return null;
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

}
