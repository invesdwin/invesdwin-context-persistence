package de.invesdwin.context.persistence.timeseriesdb.array;

import java.io.File;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.persistentmap.APersistentMap;
import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;
import de.invesdwin.context.persistence.timeseriesdb.PersistentMapType;
import de.invesdwin.util.collections.array.IPrimitiveArray;
import de.invesdwin.util.marshallers.serde.ISerde;

@ThreadSafe
public class FlyweightPrimitiveArrayPersistentMap<K> extends APersistentMap<K, IPrimitiveArray> {

    private final File directory;

    public FlyweightPrimitiveArrayPersistentMap(final String name, final File directory) {
        super(name);
        this.directory = directory;
    }

    @Override
    public File getDirectory() {
        return directory;
    }

    @Override
    public ISerde<IPrimitiveArray> newValueSerde() {
        return FlyweightPrimitiveArraySerde.GET;
    }

    @Override
    protected IPersistentMapFactory<K, IPrimitiveArray> newFactory() {
        return PersistentMapType.DISK_LARGE_FAST.newFactory();
    }

}
