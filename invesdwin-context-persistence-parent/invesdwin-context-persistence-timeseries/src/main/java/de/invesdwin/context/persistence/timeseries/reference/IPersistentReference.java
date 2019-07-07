package de.invesdwin.context.persistence.timeseries.reference;

import java.io.Closeable;

import de.invesdwin.util.concurrent.reference.IReference;

public interface IPersistentReference<T> extends IReference<T>, Closeable {

    /**
     * gets the reference, retrieving from a persistent store if it was cleared before
     */
    @Override
    T get();

    /**
     * persists the reference and removes its on-heap instance
     */
    void clear();

    /**
     * clears and deletes the persistent reference
     */
    @Override
    void close();

}
