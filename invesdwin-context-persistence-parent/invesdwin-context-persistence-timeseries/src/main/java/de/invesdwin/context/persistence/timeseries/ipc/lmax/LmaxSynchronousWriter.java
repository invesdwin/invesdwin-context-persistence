package de.invesdwin.context.persistence.timeseries.ipc.lmax;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.lmax.disruptor.RingBuffer;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousWriter;
import de.invesdwin.context.persistence.timeseries.ipc.message.EmptySynchronousMessage;
import de.invesdwin.context.persistence.timeseries.ipc.message.ISynchronousMessage;
import de.invesdwin.context.persistence.timeseries.ipc.message.MutableSynchronousMessage;

@NotThreadSafe
public class LmaxSynchronousWriter<M> implements ISynchronousWriter<M> {

    private RingBuffer<MutableSynchronousMessage<M>> ringBuffer;

    public LmaxSynchronousWriter(final RingBuffer<MutableSynchronousMessage<M>> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public void close() throws IOException {
        write(EmptySynchronousMessage.getInstance());
        ringBuffer = null;
    }

    @Override
    public void write(final int type, final int sequence, final M message) throws IOException {
        final long seq = ringBuffer.next(); // blocked by ringBuffer's gatingSequence
        final MutableSynchronousMessage<M> event = ringBuffer.get(seq);
        event.setType(type);
        event.setSequence(sequence);
        event.setMessage(message);
        ringBuffer.publish(seq);
    }

    @Override
    public void write(final ISynchronousMessage<M> message) throws IOException {
        write(message.getType(), message.getSequence(), message.getMessage());
    }

}
