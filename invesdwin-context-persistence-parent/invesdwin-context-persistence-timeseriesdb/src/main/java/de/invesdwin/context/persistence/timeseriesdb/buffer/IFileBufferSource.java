package de.invesdwin.context.persistence.timeseriesdb.buffer;

import java.io.IOException;

import de.invesdwin.util.collections.iterable.IReverseCloseableIterable;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@FunctionalInterface
public interface IFileBufferSource {

    @SuppressWarnings("rawtypes")
    IReverseCloseableIterable getSource(IByteBuffer buffer) throws IOException;

}