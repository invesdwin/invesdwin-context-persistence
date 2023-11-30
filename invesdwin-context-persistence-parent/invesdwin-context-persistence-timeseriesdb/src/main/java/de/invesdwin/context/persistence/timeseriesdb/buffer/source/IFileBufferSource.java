package de.invesdwin.context.persistence.timeseriesdb.buffer.source;

import de.invesdwin.context.persistence.timeseriesdb.IDeserializingCloseableIterable;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

public interface IFileBufferSource<V> {

    IDeserializingCloseableIterable<V> getIterable();

    ILock getReadLock();

    IByteBuffer getBuffer();

    ISerde<V> getSerde();

    Integer getFixedLength();

}