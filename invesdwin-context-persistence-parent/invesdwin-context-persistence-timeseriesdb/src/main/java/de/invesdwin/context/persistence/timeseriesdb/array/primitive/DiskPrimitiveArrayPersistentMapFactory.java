package de.invesdwin.context.persistence.timeseriesdb.array.primitive;

import java.io.File;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.persistentmap.large.LargePersistentMapFactory;
import de.invesdwin.context.integration.persistentmap.large.storage.IChunkStorage;
import de.invesdwin.context.integration.persistentmap.large.storage.SegmentedMappedFileChunkStorage;
import de.invesdwin.context.persistence.timeseriesdb.PersistentMapType;
import de.invesdwin.util.marshallers.serde.ISerde;

@Immutable
public class DiskPrimitiveArrayPersistentMapFactory<K, V> extends LargePersistentMapFactory<K, V> {

    public DiskPrimitiveArrayPersistentMapFactory() {
        super(PersistentMapType.DISK_SAFE.newFactory(), false, false);
    }

    @Override
    protected IChunkStorage<V> newChunkStorage(final File directory, final ISerde<V> valueSerde, final boolean readOnly,
            final boolean closeAllowed) {
        return new SegmentedMappedFileChunkStorage<>(directory, valueSerde, readOnly, closeAllowed);
    }

}
