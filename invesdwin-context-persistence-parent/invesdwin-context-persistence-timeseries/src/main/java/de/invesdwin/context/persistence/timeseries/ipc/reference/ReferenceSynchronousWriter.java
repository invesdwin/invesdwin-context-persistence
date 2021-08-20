package de.invesdwin.context.persistence.timeseries.ipc.reference;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousWriter;
import de.invesdwin.context.persistence.timeseries.ipc.response.EmptySynchronousResponse;
import de.invesdwin.context.persistence.timeseries.ipc.response.ISynchronousResponse;
import de.invesdwin.context.persistence.timeseries.ipc.response.ImmutableSynchronousResponse;
import de.invesdwin.util.concurrent.reference.DisabledReference;
import de.invesdwin.util.concurrent.reference.IMutableReference;

@NotThreadSafe
public class ReferenceSynchronousWriter<M> implements ISynchronousWriter<M> {

    private IMutableReference<ISynchronousResponse<M>> reference;

    public ReferenceSynchronousWriter(final IMutableReference<ISynchronousResponse<M>> reference) {
        this.reference = reference;
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public void close() throws IOException {
        reference.set(EmptySynchronousResponse.getInstance());
        reference = DisabledReference.getInstance();
    }

    @Override
    public void write(final int type, final int sequence, final M message) throws IOException {
        write(new ImmutableSynchronousResponse<M>(type, sequence, message));
    }

    @Override
    public void write(final ISynchronousResponse<M> response) throws IOException {
        reference.set(response);
    }

}
