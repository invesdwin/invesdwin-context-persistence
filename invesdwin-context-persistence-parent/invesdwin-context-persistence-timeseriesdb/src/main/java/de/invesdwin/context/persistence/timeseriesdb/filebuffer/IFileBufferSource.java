package de.invesdwin.context.persistence.timeseriesdb.filebuffer;

import java.io.IOException;

import de.invesdwin.util.collections.iterable.ICloseableIterable;

@FunctionalInterface
public interface IFileBufferSource {

    @SuppressWarnings("rawtypes")
    ICloseableIterable getSource() throws IOException;

}