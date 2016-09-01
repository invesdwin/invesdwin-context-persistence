package de.invesdwin.context.persistence.leveldb.serde.lazy;

public interface ILazySerdeValue<E> {

    byte[] getBytes();

    E getValue();

}
