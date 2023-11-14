package de.invesdwin.context.persistence.timeseriesdb.buffer.source;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.timeseriesdb.IDeserializingCloseableIterable;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@Immutable
public class IterableFileBufferSource<V> implements IFileBufferSource<V> {

    private final IDeserializingCloseableIterable<V> iterable;
    private final ILock readLock;

    public IterableFileBufferSource(final IDeserializingCloseableIterable<V> iterable, final ILock readLock) {
        this.iterable = iterable;
        this.readLock = readLock;
    }

    @Override
    public IDeserializingCloseableIterable<V> getIterable() {
        return iterable;
    }

    @Override
    public ILock getReadLock() {
        return readLock;
    }

    @Override
    public IByteBuffer getBuffer() {
        return null;
    }

    @Override
    public ISerde<V> getSerde() {
        return getIterable().getSerde();
    }

    @Override
    public Integer getFixedLength() {
        return getIterable().getFixedLength();
    }

}
