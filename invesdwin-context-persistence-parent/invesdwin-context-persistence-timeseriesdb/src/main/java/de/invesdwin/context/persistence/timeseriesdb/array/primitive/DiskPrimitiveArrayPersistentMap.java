package de.invesdwin.context.persistence.timeseriesdb.array.primitive;

import java.io.File;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.persistentmap.APersistentMap;
import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;
import de.invesdwin.util.collections.array.primitive.IPrimitiveArray;
import de.invesdwin.util.marshallers.serde.ISerde;

@ThreadSafe
public class DiskPrimitiveArrayPersistentMap<K> extends APersistentMap<K, IPrimitiveArray> {

    private final File directory;

    public DiskPrimitiveArrayPersistentMap(final String name, final File directory) {
        super(name);
        this.directory = directory;
    }

    @Override
    public File getDirectory() {
        return directory;
    }

    @Override
    public ISerde<IPrimitiveArray> newValueSerde() {
        return DiskPrimitiveArraySerde.GET;
    }

    @Override
    protected IPersistentMapFactory<K, IPrimitiveArray> newFactory() {
        return new DiskPrimitiveArrayPersistentMapFactory<>();
    }

}
