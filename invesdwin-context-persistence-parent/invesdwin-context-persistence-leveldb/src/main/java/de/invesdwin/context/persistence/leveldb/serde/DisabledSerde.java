package de.invesdwin.context.persistence.leveldb.serde;

import javax.annotation.concurrent.Immutable;

import ezdb.serde.Serde;

@Immutable
public final class DisabledSerde<E> implements Serde<E> {

    @SuppressWarnings("rawtypes")
    private static final DisabledSerde INSTANCE = new DisabledSerde();

    private DisabledSerde() {}

    @Override
    public E fromBytes(final byte[] bytes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] toBytes(final E obj) {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    public static <T> Serde<T> get() {
        return INSTANCE;
    }

}
