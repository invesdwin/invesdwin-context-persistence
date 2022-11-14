package de.invesdwin.context.persistence.chronicle;

import java.io.File;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.persistentmap.IKeyMatcher;
import de.invesdwin.context.integration.persistentmap.IPersistentMapConfig;
import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;
import de.invesdwin.util.concurrent.lock.FileChannelLock;
import de.invesdwin.util.lang.Files;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

/**
 * If you need to store large data on disk, it is better to use LevelDB only for an ordered index and store the actual
 * db in a file based persistent hash map. This is because LevelDB has very bad insertion speed when handling large
 * elements.
 */
@Immutable
public class PersistentChronicleMapFactory<K, V> implements IPersistentMapFactory<K, V> {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public ConcurrentMap<K, V> newPersistentMap(final IPersistentMapConfig<K, V> config) {
        ChronicleMapBuilder builder = ChronicleMapBuilder.of(Object.class, Object.class);
        builder.name(config.getName())
                .keyMarshaller(new ChronicleMarshaller<K>(config.newKeySerde()))
                .valueMarshaller(new ChronicleMarshaller<V>(config.newValueSerde()))
                .maxBloatFactor(1_000)
                .entries(100_000);
        builder = configureChronicleMap(config, builder);
        return createChronicleMap(config, builder);
    }

    @Override
    public boolean isDiskPersistenceSupported() {
        return true;
    }

    @SuppressWarnings("rawtypes")
    protected ChronicleMapBuilder configureChronicleMap(final IPersistentMapConfig<K, V> config,
            final ChronicleMapBuilder builder) {
        return builder.averageKeySize(100).averageValueSize(200);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected ChronicleMap<K, V> createChronicleMap(final IPersistentMapConfig<K, V> config,
            final ChronicleMapBuilder mapBuilder) {
        if (config.isDiskPersistence()) {
            final File file = new File(config.getFile(), "chronicle.map");
            try {
                Files.forceMkdirParent(file);
                //use file lock so that only one process can do a recovery at a time
                final FileChannelLock lock = new FileChannelLock(new File(config.getFile(), "chronicle.map.lock"));
                lock.tryLockThrowing(1, TimeUnit.SECONDS);
                try {
                    return mapBuilder.createPersistedTo(file);
                } finally {
                    lock.unlock();
                }
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            //off-heap
            return mapBuilder.create();
        }
    }

    @Override
    public void removeAll(final ConcurrentMap<K, V> table, final IKeyMatcher<K> matcher) {
        final ChronicleMap<K, V> cTable = (ChronicleMap<K, V>) table;
        cTable.forEachEntry((entry) -> {
            final K key = entry.key().get();
            if (matcher.matches(key)) {
                entry.doRemove();
            }
        });
    }

}
