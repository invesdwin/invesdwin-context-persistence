package de.invesdwin.context.persistence.leveldb.serde;

import java.io.Serializable;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang3.SerializationException;
import org.nustaq.serialization.simpleapi.DefaultCoder;
import org.nustaq.serialization.simpleapi.FSTCoder;

import ezdb.serde.Serde;

/**
 * This serializing serde is suitable for IPC
 */
@Immutable
public class RemoteFastSerializingSerde<E> implements Serde<E> {

    /**
     * synchronized is fine here since the serialization/deserialization is normally not the cause for multithreaded
     * slowdowns
     */
    @GuardedBy("this")
    private final FSTCoder coder;

    public RemoteFastSerializingSerde(final boolean shared, final Class<E> type) {
        if (Serializable.class.isAssignableFrom(type)) {
            this.coder = new DefaultCoder(shared, type);
        } else {
            this.coder = new DefaultCoder(shared);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized E fromBytes(final byte[] bytes) {
        try {
            return (E) coder.toObject(bytes);
        } catch (final Throwable t) {
            throw new SerializationException(t);
        }
    }

    @Override
    public synchronized byte[] toBytes(final E obj) {
        return coder.toByteArray(obj);
    }

}
