package de.invesdwin.context.persistence.chronicle;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.persistentmap.APersistentMapConfig;
import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;
import de.invesdwin.util.lang.Files;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

/**
 * If you need to store large data on disk, it is better to use LevelDB only for an ordered index and store the actual
 * db in a file based persistent hash map. This is because LevelDB has very bad insertion speed when handling large
 * elements.
 */
@Immutable
public class ChroniclePersistentMapFactory<K, V> implements IPersistentMapFactory<K, V> {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public ConcurrentMap<K, V> newPersistentMap(final APersistentMapConfig<K, V> config) {
        ChronicleMapBuilder builder = ChronicleMapBuilder.of(Object.class, Object.class);
        builder.name(config.getName())
                .keyMarshaller(new ChronicleMarshaller<K>(config.newKeySerde()))
                .valueMarshaller(new ChronicleMarshaller<V>(config.newValueSerde()))
                .maxBloatFactor(1_000)
                .entries(1_000_000);
        builder = configureChronicleMap(config, builder);
        return createChronicleMap(config, builder);
    }

    @SuppressWarnings("rawtypes")
    protected ChronicleMapBuilder configureChronicleMap(final APersistentMapConfig<K, V> config,
            final ChronicleMapBuilder builder) {
        return builder.averageKeySize(20).averageValueSize(100);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected <K, V> ChronicleMap<K, V> createChronicleMap(final APersistentMapConfig<K, V> config,
            final ChronicleMapBuilder mapBuilder) {
        if (config.isDiskPersistence()) {
            final File file = config.getFile();
            try {
                Files.forceMkdirParent(file);
                return mapBuilder.createOrRecoverPersistedTo(file);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            return mapBuilder.create();
        }
    }

}
