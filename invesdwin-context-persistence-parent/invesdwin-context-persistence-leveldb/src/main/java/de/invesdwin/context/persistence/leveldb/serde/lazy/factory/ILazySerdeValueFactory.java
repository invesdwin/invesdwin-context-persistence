package de.invesdwin.context.persistence.leveldb.serde.lazy.factory;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.leveldb.serde.lazy.ILazySerdeValue;
import ezdb.serde.Serde;

@NotThreadSafe
public interface ILazySerdeValueFactory {

    <T> ILazySerdeValue<T> fromValue(Serde<T> serde, T value);

    <T> ILazySerdeValue<T> fromBytes(final Serde<T> serde, byte[] bytes);

}
