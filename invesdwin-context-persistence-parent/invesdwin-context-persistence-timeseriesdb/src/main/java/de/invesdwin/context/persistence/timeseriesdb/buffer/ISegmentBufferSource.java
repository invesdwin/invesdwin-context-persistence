package de.invesdwin.context.persistence.timeseriesdb.buffer;

import java.io.IOException;

import de.invesdwin.util.collections.iterable.IReverseCloseableIterable;

@FunctionalInterface
public interface ISegmentBufferSource {

    @SuppressWarnings("rawtypes")
    IReverseCloseableIterable getSource() throws IOException;

}