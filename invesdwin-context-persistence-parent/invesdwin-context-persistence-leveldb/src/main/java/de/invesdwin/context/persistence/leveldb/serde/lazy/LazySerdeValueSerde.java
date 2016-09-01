package de.invesdwin.context.persistence.leveldb.serde.lazy;

import javax.annotation.concurrent.NotThreadSafe;

import ezdb.serde.Serde;

@NotThreadSafe
public class LazySerdeValueSerde<E> implements Serde<ILazySerdeValue<E>> {

    private final Serde<E> delegate;

    public LazySerdeValueSerde(final Serde<E> delegate) {
        this.delegate = delegate;
    }

    @Override
    public ILazySerdeValue<E> fromBytes(final byte[] bytes) {
        return new LazySerdeValue<E>(delegate, bytes);
    }

    @Override
    public byte[] toBytes(final ILazySerdeValue<E> obj) {
        return obj.getBytes();
    }

}
