package de.invesdwin.context.persistence.timeseries.ipc.lmax;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.lmax.disruptor.EventPoller;
import com.lmax.disruptor.RingBuffer;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousReader;
import de.invesdwin.context.persistence.timeseries.ipc.response.EmptySynchronousResponse;
import de.invesdwin.context.persistence.timeseries.ipc.response.ISynchronousResponse;
import de.invesdwin.context.persistence.timeseries.ipc.response.ImmutableSynchronousResponse;
import de.invesdwin.context.persistence.timeseries.ipc.response.MutableSynchronousResponse;

@NotThreadSafe
public class LmaxSynchronousReader<M> implements ISynchronousReader<M> {

    private RingBuffer<MutableSynchronousResponse<M>> ringBuffer;
    private EventPoller<MutableSynchronousResponse<M>> eventPoller;
    private ImmutableSynchronousResponse<M> polledValue;

    private final EventPoller.Handler<MutableSynchronousResponse<M>> pollerHandler = (event, sequence, endOfBatch) -> {
        polledValue = ImmutableSynchronousResponse.valueOf(event);
        return false;
    };

    public LmaxSynchronousReader(final RingBuffer<MutableSynchronousResponse<M>> ringBuffer) {
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
    public ISynchronousResponse<M> readMessage() throws IOException {
        final ISynchronousResponse<M> message = getPolledMessage();
        if (message.getType() == EmptySynchronousResponse.TYPE) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

    private ISynchronousResponse<M> getPolledMessage() {
        if (polledValue != null) {
            final ImmutableSynchronousResponse<M> value = polledValue;
            polledValue = null;
            return value;
        }
        try {
            final EventPoller.PollState poll = eventPoller.poll(pollerHandler);
            if (poll == EventPoller.PollState.PROCESSING) {
                final ImmutableSynchronousResponse<M> value = polledValue;
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
