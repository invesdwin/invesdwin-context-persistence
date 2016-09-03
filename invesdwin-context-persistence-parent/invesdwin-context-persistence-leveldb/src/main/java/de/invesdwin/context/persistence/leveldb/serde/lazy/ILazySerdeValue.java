package de.invesdwin.context.persistence.leveldb.serde.lazy;

import de.invesdwin.util.collections.iterable.ATransformingCloseableIterable;
import de.invesdwin.util.collections.iterable.ATransformingCloseableIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.buffer.ATransformingBufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import ezdb.serde.Serde;

public interface ILazySerdeValue<E> {

    Serde<? extends E> getDelegate();

    byte[] getBytes();

    E getValue();

    static <T> T getValue(final ILazySerdeValue<T> value) {
        if (value == null) {
            return null;
        } else {
            return value.getValue();
        }
    }

    static byte[] getBytes(final ILazySerdeValue<?> value) {
        if (value == null) {
            return null;
        } else {
            return value.getBytes();
        }
    }

    static <T> IBufferingIterator<? extends T> unlazy(
            final IBufferingIterator<? extends ILazySerdeValue<? extends T>> delegate) {
        return new ATransformingBufferingIterator<ILazySerdeValue<? extends T>, T>(delegate) {
            @Override
            protected T transformSource(final ILazySerdeValue<? extends T> value) {
                return value.getValue();
            }

            @Override
            protected ILazySerdeValue<? extends T> transformResult(final T value) {
                throw new UnsupportedOperationException();
            }
        };
    }

    static <T> T unlazy(final ILazySerdeValue<? extends T> delegate) {
        return getValue(delegate);
    }

    static <T> ICloseableIterable<? extends T> unlazy(
            final ICloseableIterable<? extends ILazySerdeValue<? extends T>> delegate) {
        return new ATransformingCloseableIterable<ILazySerdeValue<? extends T>, T>(delegate) {

            @Override
            protected T transform(final ILazySerdeValue<? extends T> value) {
                return value.getValue();
            }
        };
    }

    static <T> ICloseableIterator<? extends T> unlazy(
            final ICloseableIterator<? extends ILazySerdeValue<? extends T>> delegate) {
        return new ATransformingCloseableIterator<ILazySerdeValue<? extends T>, T>(delegate) {

            @Override
            protected T transform(final ILazySerdeValue<? extends T> value) {
                return value.getValue();
            }
        };
    }

}
