package de.invesdwin.context.persistence.krati;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.persistentmap.IKeyMatcher;
import de.invesdwin.context.integration.persistentmap.IPersistentMapConfig;
import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;
import de.invesdwin.util.lang.Files;
import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.core.StoreParams;
import krati.core.segment.WriteBufferSegmentFactory;
import krati.store.DynamicDataStore;

@Immutable
public class PersistentKratiMapFactory<K, V> implements IPersistentMapFactory<K, V> {

    @Override
    public ConcurrentMap<K, V> newPersistentMap(final IPersistentMapConfig<K, V> config) {
        try {
            Files.forceMkdirParent(config.getFile());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        final DynamicDataStore dataStore;
        try {
            dataStore = StoreFactory.createDynamicDataStore(newStoreConfig(config));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        return new KratiMap<>(dataStore, config.newKeySerde(), config.newValueSerde());
    }

    /**
     * https://github.com/jingwei/krati/blob/master/krati-main/src/examples/java/krati/examples/LargeStore.java
     */
    protected StoreConfig newStoreConfig(final IPersistentMapConfig<K, V> config) {
        final int initialCapacity = 100_000;
        final StoreConfig storeConfig;
        try {
            storeConfig = new StoreConfig(config.getFile(), initialCapacity);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        storeConfig.setBatchSize(10_000);
        storeConfig.setNumSyncBatches(100);

        // Configure store segments
        storeConfig.setSegmentFactory(new WriteBufferSegmentFactory());
        storeConfig.setSegmentFileSizeMB(128);
        storeConfig.setSegmentCompactFactor(0.67);

        // Configure index segments
        storeConfig.setInt(StoreParams.PARAM_INDEX_SEGMENT_FILE_SIZE_MB, 32);
        storeConfig.setDouble(StoreParams.PARAM_INDEX_SEGMENT_COMPACT_FACTOR, 0.5);

        // Configure index initial capacity
        final int indexInitialCapacity = initialCapacity / 8;
        storeConfig.setInt(StoreParams.PARAM_INDEX_INITIAL_CAPACITY, indexInitialCapacity);

        // Disable linear hashing
        storeConfig.setHashLoadFactor(1.0);
        return storeConfig;
    }

    @Override
    public void removeAll(final ConcurrentMap<K, V> table, final IKeyMatcher<K> matcher) {
        for (final Entry<K, V> e : table.entrySet()) {
            final K key = e.getKey();
            if (matcher.matches(key)) {
                table.remove(key);
            }
        }
    }

}
