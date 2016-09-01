package de.invesdwin.context.persistence.leveldb.serde.lazy;

import java.util.concurrent.Callable;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.concurrent.ACachedCallable;
import ezdb.serde.Serde;

@NotThreadSafe
public abstract class ATransformingLazySerdeValue<S, R> implements ILazySerdeValue<R> {

    private final Callable<R> value;
    private Callable<byte[]> bytes;

    public ATransformingLazySerdeValue(final Serde<R> serde, final ILazySerdeValue<? extends S> delegate) {
        this.value = new ACachedCallable<R>() {
            @Override
            protected R innerCall() {
                return transform(delegate.getValue());
            }
        };
        this.bytes = new ACachedCallable<byte[]>() {
            @Override
            protected byte[] innerCall() {
                return serde.toBytes(getValue());
            }
        };
    }

    protected abstract R transform(S value);

    @Override
    public R getValue() {
        try {
            return value.call();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] getBytes() {
        try {
            return bytes.call();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

}
