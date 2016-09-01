package de.invesdwin.context.persistence.leveldb.serde.lazy;

import java.util.concurrent.Callable;

import javax.annotation.concurrent.NotThreadSafe;

import ezdb.serde.Serde;

@NotThreadSafe
public class LazySerdeValue<E> implements ILazySerdeValue<E> {

    private final Callable<byte[]> bytesCallable;
    private final E value;

    public LazySerdeValue(final Serde<E> serde, final E value) {
        this.bytesCallable = new Callable<byte[]>() {
            @Override
            public byte[] call() {
                return serde.toBytes(value);
            }
        };
        this.value = value;
    }

    public LazySerdeValue(final Serde<E> serde, final byte[] bytes) {
        final byte[] bytesCopy = bytes.clone();
        this.bytesCallable = new Callable<byte[]>() {
            @Override
            public byte[] call() throws Exception {
                return bytesCopy;
            }
        };
        this.value = serde.fromBytes(bytes);
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

}
