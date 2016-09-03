package de.invesdwin.context.persistence.leveldb.serde.lazy;

import javax.annotation.concurrent.NotThreadSafe;

import ezdb.serde.Serde;

@NotThreadSafe
public class LocalLazySerdeValue<E> implements ILazySerdeValue<E> {

    private final Serde<? extends E> delegate;
    private final E value;

    public LocalLazySerdeValue(final Serde<? extends E> delegate, final E value) {
        this.delegate = delegate;
        this.value = value;
    }

    public LocalLazySerdeValue(final Serde<? extends E> delegate, final byte[] bytes) {
        this.delegate = delegate;
        this.value = delegate.fromBytes(bytes);
    }

    @Override
    public Serde<? extends E> getDelegate() {
        return delegate;
    }

    @Override
    public byte[] getBytes() {
        throw new UnsupportedOperationException(LocalLazySerdeValue.class.getSimpleName()
                + " should only be used when the bytes should be discarded, because they won't be sent over the wire");
    }

    @Override
    public E getValue() {
        return value;
    }

}
