package de.invesdwin.context.persistence.leveldb.serde.lazy;

import java.util.concurrent.Callable;

import javax.annotation.concurrent.NotThreadSafe;

import ezdb.serde.Serde;

@NotThreadSafe
public abstract class ATransformingLazySerdeValue<S, R> implements ILazySerdeValue<R> {

    private Callable<R> valueCallable;
    private Callable<byte[]> bytesCallable;

    public ATransformingLazySerdeValue(final Serde<R> serde, final ILazySerdeValue<? extends S> delegate) {
        this.valueCallable = new Callable<R>() {
            @Override
            public R call() {
                final R value = transform(delegate.getValue());
                valueCallable = new Callable<R>() {
                    @Override
                    public R call() throws Exception {
                        return value;
                    }
                };
                return value;
            }
        };
        this.bytesCallable = new Callable<byte[]>() {
            @Override
            public byte[] call() {
                final byte[] bytes = serde.toBytes(getValue());
                bytesCallable = new Callable<byte[]>() {
                    @Override
                    public byte[] call() throws Exception {
                        return bytes;
                    }
                };
                return bytes;
            }
        };
    }

    protected abstract R transform(S value);

    @Override
    public R getValue() {
        try {
            return valueCallable.call();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] getBytes() {
        try {
            return bytesCallable.call();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

}
