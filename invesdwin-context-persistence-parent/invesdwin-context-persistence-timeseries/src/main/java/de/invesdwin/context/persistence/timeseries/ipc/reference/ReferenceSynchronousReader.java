package de.invesdwin.context.persistence.timeseries.ipc.reference;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ipc.ISynchronousReader;
import de.invesdwin.context.persistence.timeseries.ipc.message.EmptySynchronousMessage;
import de.invesdwin.context.persistence.timeseries.ipc.message.ISynchronousMessage;
import de.invesdwin.util.concurrent.reference.DisabledReference;
import de.invesdwin.util.concurrent.reference.IMutableReference;

@NotThreadSafe
public class ReferenceSynchronousReader<M> implements ISynchronousReader<M> {

    private IMutableReference<ISynchronousMessage<M>> reference;

    public ReferenceSynchronousReader(final IMutableReference<ISynchronousMessage<M>> reference) {
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
    public ISynchronousMessage<M> readMessage() throws IOException {
        final ISynchronousMessage<M> message = reference.getAndSet(null);
        if (message == EmptySynchronousMessage.getInstance()) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

}
