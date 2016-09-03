package de.invesdwin.context.persistence.leveldb.serde.lazy;

import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import ezdb.serde.Serde;

@NotThreadSafe
public class RemoteLazySerdeValueSerde<E> implements ILazySerdeValueSerde<E> {

    private final Serde<? extends E> delegate;
    private final Function<byte[], ILazySerdeValue<E>> fromBytes;

    public RemoteLazySerdeValueSerde(final Serde<? extends E> delegate, final boolean clone) {
        this.delegate = ILazySerdeValueSerde.maybeUnwrap(delegate);
        if (clone) {
            this.fromBytes = new Function<byte[], ILazySerdeValue<E>>() {
                @Override
                public ILazySerdeValue<E> apply(final byte[] t) {
                    return RemoteLazySerdeValue.fromBytesClone(delegate, t);
                }
            };
        } else {
            this.fromBytes = new Function<byte[], ILazySerdeValue<E>>() {
                @Override
                public ILazySerdeValue<E> apply(final byte[] t) {
                    return RemoteLazySerdeValue.fromBytes(delegate, t);
                }
            };
        }
    }

    @Override
    public ILazySerdeValue<E> fromBytes(final byte[] bytes) {
        return fromBytes.apply(bytes);
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
        if (lazySerdeValue instanceof RemoteLazySerdeValue) {
            return lazySerdeValue;
        } else {
            throw new UnsupportedOperationException("You should use " + getClass().getSimpleName() + " directly");
        }
    }

}
