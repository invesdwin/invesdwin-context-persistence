package de.invesdwin.context.persistence.timeseries.ipc;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.timeseries.ipc.response.ISynchronousResponse;

@Immutable
public class ClosedSynchronousReader<M> implements ISynchronousReader<M> {

    @SuppressWarnings("rawtypes")
    private static final ClosedSynchronousReader INSTANCE = new ClosedSynchronousReader<>();

    public static <T> ClosedSynchronousReader<T> getInstance() {
        return INSTANCE;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public ISynchronousResponse<M> readMessage() {
        return null;
    }

    @Override
    public boolean hasNext() throws IOException {
        throw new EOFException();
    }
}
