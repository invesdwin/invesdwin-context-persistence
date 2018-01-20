package de.invesdwin.context.persistence.leveldb.serde;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.codec.binary.Base64;

import ezdb.serde.Serde;

@Immutable
public class Base64DelegateSerde<E> implements Serde<E> {

    private final Serde<E> delegate;

    public Base64DelegateSerde(final Serde<E> delegate) {
        this.delegate = delegate;
    }

    @Override
    public E fromBytes(final byte[] bytes) {
        final byte[] decodedBytes = Base64.decodeBase64(bytes);
        return delegate.fromBytes(decodedBytes);
    }

    @Override
    public byte[] toBytes(final E obj) {
        final byte[] bytes = delegate.toBytes(obj);
        final byte[] encodedBytes = Base64.encodeBase64(bytes);
        return encodedBytes;
    }

}
