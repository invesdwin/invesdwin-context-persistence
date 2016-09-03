package de.invesdwin.context.persistence.leveldb.serde.lazy;

import javax.annotation.concurrent.NotThreadSafe;

import ezdb.serde.Serde;

@NotThreadSafe
public abstract class ATransformingLazySerdeValue<S, R> implements ILazySerdeValue<R> {

    private final Serde<R> serde;
    private final R value;

    public ATransformingLazySerdeValue(final Serde<R> serde, final ILazySerdeValue<? extends S> delegate) {
        this.serde = serde;
        this.value = transform(delegate.getValue());
    }

    protected abstract R transform(S value);

    @Override
    public R getValue() {
        return value;
    }

    @Override
    public byte[] getBytes() {
        return serde.toBytes(getValue());
    }

    @Override
    public Serde<? extends R> getDelegate() {
        return serde;
    }

}
