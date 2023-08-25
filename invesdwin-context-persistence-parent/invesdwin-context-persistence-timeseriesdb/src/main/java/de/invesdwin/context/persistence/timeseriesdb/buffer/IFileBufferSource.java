package de.invesdwin.context.persistence.timeseriesdb.buffer;

import de.invesdwin.context.persistence.timeseriesdb.IDeserializingCloseableIterable;

@FunctionalInterface
public interface IFileBufferSource {

    @SuppressWarnings("rawtypes")
    IDeserializingCloseableIterable getSource();

}