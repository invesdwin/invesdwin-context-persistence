package de.invesdwin.context.persistence.timeseriesdb.array.large;

import java.io.File;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.persistentmap.APersistentMap;
import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;
import de.invesdwin.util.collections.array.large.ILargeArray;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.large.ILargeSerde;

@ThreadSafe
public class DiskLargeArrayPersistentMap<K> extends APersistentMap<K, ILargeArray> {

    private final File directory;

    public DiskLargeArrayPersistentMap(final String name, final File directory) {
        super(name);
        this.directory = directory;
    }

    @Override
    public File getDirectory() {
        return directory;
    }

    @Override
    public ILargeSerde<ILargeArray> newLargeValueSerde() {
        return DiskLargeArraySerde.GET;
    }

    @Override
    public ISerde<ILargeArray> newValueSerde() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected IPersistentMapFactory<K, ILargeArray> newFactory() {
        return new DiskLargeArrayPersistentMapFactory<>();
    }

}
