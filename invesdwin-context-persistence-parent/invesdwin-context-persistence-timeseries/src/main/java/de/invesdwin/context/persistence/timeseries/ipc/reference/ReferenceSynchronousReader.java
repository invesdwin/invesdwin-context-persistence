package de.invesdwin.context.persistence.timeseries.ipc.reference;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousReader;
import de.invesdwin.context.persistence.timeseries.ipc.response.EmptySynchronousResponse;
import de.invesdwin.context.persistence.timeseries.ipc.response.ISynchronousResponse;
import de.invesdwin.util.concurrent.reference.DisabledReference;
import de.invesdwin.util.concurrent.reference.IMutableReference;

@NotThreadSafe
public class ReferenceSynchronousReader<M> implements ISynchronousReader<M> {

    private IMutableReference<ISynchronousResponse<M>> reference;

    public ReferenceSynchronousReader(final IMutableReference<ISynchronousResponse<M>> reference) {
        this.reference = reference;
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public void close() throws IOException {
        reference = DisabledReference.getInstance();
    }

    @Override
    public boolean hasNext() throws IOException {
        return reference.get() != null;
    }

    @Override
    public ISynchronousResponse<M> readMessage() throws IOException {
        final ISynchronousResponse<M> message = reference.getAndSet(null);
        if (message == EmptySynchronousResponse.getInstance()) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

}
