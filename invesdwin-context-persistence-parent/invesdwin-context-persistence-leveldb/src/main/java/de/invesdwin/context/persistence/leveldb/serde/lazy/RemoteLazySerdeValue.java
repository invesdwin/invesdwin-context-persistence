package de.invesdwin.context.persistence.leveldb.serde.lazy;

import java.util.concurrent.Callable;

import javax.annotation.concurrent.NotThreadSafe;

import ezdb.serde.Serde;

@NotThreadSafe
public final class RemoteLazySerdeValue<E> implements ILazySerdeValue<E> {

    private final Serde<? extends E> delegate;
    private final Callable<byte[]> bytesCallable;
    private final E value;

    private RemoteLazySerdeValue(final Serde<E> delegate, final E value) {
        this.delegate = delegate;
        this.bytesCallable = new Callable<byte[]>() {
            @Override
            public byte[] call() {
                return delegate.toBytes(value);
            }
        };
        this.value = value;
    }

    private RemoteLazySerdeValue(final Serde<E> delegate, final byte[] bytes) {
        this.delegate = delegate;
        this.bytesCallable = new Callable<byte[]>() {
            @Override
            public byte[] call() throws Exception {
                return bytes;
            }
        };
        this.value = delegate.fromBytes(bytes);
    }

    @Override
    public Serde<? extends E> getDelegate() {
        return delegate;
    }

    @Override
    public byte[] getBytes() {
        try {
            return bytesCallable.call();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public E getValue() {
        return value;
    }

    public static <T> T getValue(final ILazySerdeValue<T> value) {
        if (value == null) {
            return null;
        } else {
            return value.getValue();
        }
    }

    public static byte[] getBytes(final ILazySerdeValue<?> value) {
        if (value == null) {
            return null;
        } else {
            return value.getBytes();
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> RemoteLazySerdeValue<T> fromBytes(final Serde<? extends T> serde, final byte[] bytes) {
        return new RemoteLazySerdeValue<T>((Serde<T>) serde, bytes);
    }

    @SuppressWarnings("unchecked")
    public static <T> RemoteLazySerdeValue<T> fromBytesClone(final Serde<? extends T> serde, final byte[] bytes) {
        /*
         * this clone is needed since the provider might want to cache its byte buffer; it is also a valid performance
         * sacrifice to skip serializing again when the data might get pushed to another process via IPC
         */
        final byte[] bytesCopy = bytes.clone();
        return new RemoteLazySerdeValue<T>((Serde<T>) serde, bytesCopy);
    }

    public static <T> RemoteLazySerdeValue<T> fromValue(final Serde<T> serde, final T value) {
        return new RemoteLazySerdeValue<T>(serde, value);
    }

}
