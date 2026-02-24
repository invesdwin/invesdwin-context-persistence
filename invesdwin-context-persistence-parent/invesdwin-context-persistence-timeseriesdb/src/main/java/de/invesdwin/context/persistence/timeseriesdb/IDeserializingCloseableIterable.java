package de.invesdwin.context.persistence.timeseriesdb;

import java.io.IOException;
import java.io.InputStream;

import de.invesdwin.util.collections.iterable.IReverseCloseableIterable;
import de.invesdwin.util.marshallers.serde.ISerde;

public interface IDeserializingCloseableIterable<E> extends IReverseCloseableIterable<E> {

    String getName();

    int size();

    ISerde<E> getSerde();

    Integer getFixedLength();

    InputStream newInputStream() throws IOException;

}
