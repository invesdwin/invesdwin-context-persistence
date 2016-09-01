package de.invesdwin.context.persistence.leveldb.serde.lazy;

import java.util.concurrent.Callable;

import javax.annotation.concurrent.NotThreadSafe;

import ezdb.serde.Serde;

@NotThreadSafe
public class LazySerdeValue<E> implements ILazySerdeValue<E> {

    private Callable<byte[]> bytesCallable;
    private Callable<E> valueCallable;

    public LazySerdeValue(final Serde<E> serde, final E value) {
        this.bytesCallable = new Callable<byte[]>() {
            @Override
            public byte[] call() {
                final byte[] bytes = serde.toBytes(value);
                bytesCallable = new Callable<byte[]>() {
                    @Override
                    public byte[] call() throws Exception {
                        return bytes;
                    }
                };
                return bytes;
            }
        };
        this.valueCallable = new Callable<E>() {
            @Override
            public E call() throws Exception {
                return value;
            }
        };
    }

    public LazySerdeValue(final Serde<E> serde, final byte[] bytes) {
        this.bytesCallable = new Callable<byte[]>() {
            @Override
            public byte[] call() throws Exception {
                return bytes;
            }
        };
        this.valueCallable = new Callable<E>() {
            @Override
            public E call() {
                final E value = serde.fromBytes(bytes);
                valueCallable = new Callable<E>() {
                    @Override
                    public E call() throws Exception {
                        return value;
                    }
                };
                return value;
            }
        };
    }

    public LazySerdeValue(final Serde<E> serde, final Callable<E> value) {
        this.bytesCallable = new Callable<byte[]>() {
            @Override
            public byte[] call() {
                try {
                    final byte[] bytes = serde.toBytes(value.call());
                    bytesCallable = new Callable<byte[]>() {
                        @Override
                        public byte[] call() throws Exception {
                            return bytes;
                        }
                    };
                    return bytes;
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
        this.valueCallable = value;
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
        try {
            return valueCallable.call();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
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
