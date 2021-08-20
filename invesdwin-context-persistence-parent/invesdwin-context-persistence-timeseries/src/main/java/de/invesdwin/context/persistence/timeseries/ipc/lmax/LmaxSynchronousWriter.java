package de.invesdwin.context.persistence.timeseries.ipc.lmax;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.lmax.disruptor.RingBuffer;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousWriter;
import de.invesdwin.context.persistence.timeseries.ipc.response.EmptySynchronousResponse;
import de.invesdwin.context.persistence.timeseries.ipc.response.ISynchronousResponse;
import de.invesdwin.context.persistence.timeseries.ipc.response.MutableSynchronousResponse;

@NotThreadSafe
public class LmaxSynchronousWriter<M> implements ISynchronousWriter<M> {

    private RingBuffer<MutableSynchronousResponse<M>> ringBuffer;

    public LmaxSynchronousWriter(final RingBuffer<MutableSynchronousResponse<M>> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public void close() throws IOException {
        write(EmptySynchronousResponse.getInstance());
        ringBuffer = null;
    }

    @Override
    public void write(final int type, final int sequence, final M message) throws IOException {
        final long seq = ringBuffer.next(); // blocked by ringBuffer's gatingSequence
        final MutableSynchronousResponse<M> event = ringBuffer.get(seq);
        event.setType(type);
        event.setSequence(sequence);
        event.setMessage(message);
        ringBuffer.publish(seq);
    }

    @Override
    public void write(final ISynchronousResponse<M> response) throws IOException {
        write(response.getType(), response.getSequence(), response.getMessage());
    }

}
