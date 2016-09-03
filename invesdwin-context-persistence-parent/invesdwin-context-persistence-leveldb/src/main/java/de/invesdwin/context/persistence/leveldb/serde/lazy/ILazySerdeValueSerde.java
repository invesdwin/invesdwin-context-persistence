package de.invesdwin.context.persistence.leveldb.serde.lazy;

import ezdb.serde.Serde;

public interface ILazySerdeValueSerde<E> extends Serde<ILazySerdeValue<? extends E>> {

    Serde<E> getDelegate();

    ILazySerdeValue<? extends E> maybeRewrap(ILazySerdeValue<? extends E> lazySerdeValue);

    @SuppressWarnings("unchecked")
    static <T> Serde<? extends T> maybeUnwrap(final Serde<T> serde) {
        if (serde instanceof ILazySerdeValueSerde) {
            final ILazySerdeValueSerde<T> lazy = (ILazySerdeValueSerde<T>) serde;
            return lazy.getDelegate();
        } else {
            return serde;
        }
    }

}
