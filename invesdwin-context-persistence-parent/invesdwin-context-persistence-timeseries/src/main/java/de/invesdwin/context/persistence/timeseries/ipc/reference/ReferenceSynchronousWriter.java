package de.invesdwin.context.persistence.timeseries.ipc.reference;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousWriter;
import de.invesdwin.context.persistence.timeseries.ipc.message.EmptySynchronousMessage;
import de.invesdwin.context.persistence.timeseries.ipc.message.ISynchronousMessage;
import de.invesdwin.context.persistence.timeseries.ipc.message.ImmutableSynchronousMessage;
import de.invesdwin.util.concurrent.reference.DisabledReference;
import de.invesdwin.util.concurrent.reference.IMutableReference;

@NotThreadSafe
public class ReferenceSynchronousWriter<M> implements ISynchronousWriter<M> {

    private IMutableReference<ISynchronousMessage<M>> reference;

    public ReferenceSynchronousWriter(final IMutableReference<ISynchronousMessage<M>> reference) {
        this.reference = reference;
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public void close() throws IOException {
        reference.set(EmptySynchronousMessage.getInstance());
        reference = DisabledReference.getInstance();
    }

    @Override
    public void write(final int type, final int sequence, final M message) throws IOException {
        write(new ImmutableSynchronousMessage<M>(type, sequence, message));
    }

    @Override
    public void write(final ISynchronousMessage<M> message) throws IOException {
        reference.set(message);
    }

}
