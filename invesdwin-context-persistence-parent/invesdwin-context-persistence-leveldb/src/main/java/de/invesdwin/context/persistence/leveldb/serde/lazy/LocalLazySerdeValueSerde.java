package de.invesdwin.context.persistence.leveldb.serde.lazy;

import javax.annotation.concurrent.NotThreadSafe;

import ezdb.serde.Serde;

@NotThreadSafe
public class LocalLazySerdeValueSerde<E> implements ILazySerdeValueSerde<E> {

    private final Serde<? extends E> delegate;

    public LocalLazySerdeValueSerde(final Serde<? extends E> delegate) {
        this.delegate = ILazySerdeValueSerde.maybeUnwrap(delegate);
    }

    @Override
    public LocalLazySerdeValue<E> fromBytes(final byte[] bytes) {
        return new LocalLazySerdeValue<E>(delegate, bytes);
    }

    @Override
    public byte[] toBytes(final ILazySerdeValue<? extends E> obj) {
        return obj.getBytes();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Serde<E> getDelegate() {
        return (Serde<E>) delegate;
    }

    @Override
    public ILazySerdeValue<? extends E> maybeRewrap(final ILazySerdeValue<? extends E> lazySerdeValue) {
        if (lazySerdeValue instanceof LocalLazySerdeValue) {
            return lazySerdeValue;
        } else {
            return new LocalLazySerdeValue<E>(lazySerdeValue.getDelegate(), lazySerdeValue.getValue());
        }
    }

}
