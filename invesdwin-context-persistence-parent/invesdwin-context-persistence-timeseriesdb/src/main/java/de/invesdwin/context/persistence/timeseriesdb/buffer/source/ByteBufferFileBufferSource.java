package de.invesdwin.context.persistence.timeseriesdb.buffer.source;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.timeseriesdb.IDeserializingCloseableIterable;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.disabled.DisabledLock;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@Immutable
public class ByteBufferFileBufferSource<V> implements IFileBufferSource<V> {

    private final IByteBuffer buffer;
    private final ISerde<V> serde;
    private final Integer fixedLength;

    public ByteBufferFileBufferSource(final IByteBuffer buffer, final ISerde<V> serde, final Integer fixedLength) {
        this.buffer = buffer;
        this.serde = serde;
        this.fixedLength = fixedLength;
    }

    @Override
    public IDeserializingCloseableIterable<V> getIterable() {
        return null;
    }

    @Override
    public ILock getReadLock() {
        return DisabledLock.INSTANCE;
    }

    @Override
    public IByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public ISerde<V> getSerde() {
        return serde;
    }

    @Override
    public Integer getFixedLength() {
        return fixedLength;
    }

}
