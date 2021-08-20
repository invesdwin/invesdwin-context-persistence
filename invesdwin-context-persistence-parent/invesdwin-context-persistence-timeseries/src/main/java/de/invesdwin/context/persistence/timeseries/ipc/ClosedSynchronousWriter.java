package de.invesdwin.context.persistence.timeseries.ipc;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.timeseries.ipc.message.ISynchronousMessage;

@Immutable
public class ClosedSynchronousWriter<M> implements ISynchronousWriter<M> {

    @SuppressWarnings("rawtypes")
    private static final ClosedSynchronousWriter INSTANCE = new ClosedSynchronousWriter<>();

    @SuppressWarnings("unchecked")
    public static final <T> ClosedSynchronousWriter<T> getInstance() {
        return INSTANCE;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public void write(final int type, final int sequence, final Object message) {
    }

    @Override
    public void write(final ISynchronousMessage<M> message) throws IOException {
    }
}
