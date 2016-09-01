package de.invesdwin.context.persistence.leveldb.serde.lazy;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.leveldb.serde.lazy.factory.ILazySerdeValueFactory;
import ezdb.serde.Serde;

@NotThreadSafe
public class LazySerdeValueSerde<E> implements Serde<ILazySerdeValue<E>> {

    private final Serde<E> delegate;
    private final ILazySerdeValueFactory factory;

    public LazySerdeValueSerde(final Serde<E> delegate, final ILazySerdeValueFactory factory) {
        this.delegate = delegate;
        this.factory = factory;
    }

    @Override
    public ILazySerdeValue<E> fromBytes(final byte[] bytes) {
        return factory.fromBytes(delegate, bytes);
    }

    @Override
    public byte[] toBytes(final ILazySerdeValue<E> obj) {
        return obj.getBytes();
    }

}
