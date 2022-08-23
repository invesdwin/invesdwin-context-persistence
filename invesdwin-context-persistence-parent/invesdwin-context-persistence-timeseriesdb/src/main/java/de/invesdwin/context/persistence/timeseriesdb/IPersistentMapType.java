package de.invesdwin.context.persistence.timeseriesdb;

import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;

public interface IPersistentMapType {

    <K, V> IPersistentMapFactory<K, V> newFactory();

    /**
     * e.g. ChronicleMap does not free disk space when remove is called, so clear() should be used instead.
     */
    boolean isRemoveFullySupported();

}
