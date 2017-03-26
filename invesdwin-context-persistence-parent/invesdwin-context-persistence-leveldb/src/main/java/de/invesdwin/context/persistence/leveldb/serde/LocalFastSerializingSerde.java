package de.invesdwin.context.persistence.leveldb.serde;

import java.io.Serializable;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.lang.Objects;
import ezdb.serde.Serde;

/**
 * This serializing serde is only suitable inside the current JVM
 */
@Immutable
public class LocalFastSerializingSerde<E extends Serializable> implements Serde<E> {

    @SuppressWarnings("rawtypes")
    private static final LocalFastSerializingSerde INSTANCE = new LocalFastSerializingSerde<>();

    @Override
    public E fromBytes(final byte[] bytes) {
        return Objects.deserialize(bytes);
    }

    @Override
    public byte[] toBytes(final E obj) {
        return Objects.serialize(obj);
    }

    @SuppressWarnings("unchecked")
    public static <T extends Serializable> LocalFastSerializingSerde<T> get() {
        return INSTANCE;
    }

}
