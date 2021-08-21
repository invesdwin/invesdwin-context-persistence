package de.invesdwin.context.persistence.timeseries.ezdb;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.serde.ISerde;

@Immutable
public class EzdbSerde<O> implements ezdb.serde.Serde<O> {

    private final ISerde<O> delegate;

    public EzdbSerde(final ISerde<O> delegate) {
        this.delegate = delegate;
    }

    @Override
    public O fromBytes(final byte[] bytes) {
        return delegate.fromBytes(bytes);
    }

    @Override
    public byte[] toBytes(final O obj) {
        return delegate.toBytes(obj);
    }

    @SuppressWarnings("unchecked")
    public static <T> ezdb.serde.Serde<T> valueOf(final ISerde<T> delegate) {
        if (delegate == null) {
            return null;
        } else if (delegate instanceof ezdb.serde.Serde) {
            return (ezdb.serde.Serde<T>) delegate;
        } else {
            return new EzdbSerde<T>(delegate);
        }
    }

}
