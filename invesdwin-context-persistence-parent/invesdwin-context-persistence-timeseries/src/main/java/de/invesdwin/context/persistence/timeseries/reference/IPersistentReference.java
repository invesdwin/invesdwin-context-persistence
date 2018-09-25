package de.invesdwin.context.persistence.timeseries.reference;

import java.io.Closeable;

public interface IPersistentReference<T> extends Closeable {

    /**
     * gets the reference, retrieving from a persistent store if it was cleared before
     */
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
