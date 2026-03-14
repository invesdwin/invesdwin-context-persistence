package de.invesdwin.context.persistence.mapdb;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;

import javax.annotation.concurrent.ThreadSafe;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DB.TreeMapMaker;
import org.mapdb.DBMaker;
import org.mapdb.DBMaker.Maker;

import de.invesdwin.context.integration.persistentmap.IKeyMatcher;
import de.invesdwin.context.integration.persistentmap.IPersistentMapConfig;
import de.invesdwin.context.integration.persistentmap.navigable.IPersistentNavigableMapConfig;
import de.invesdwin.context.integration.persistentmap.navigable.IPersistentNavigableMapFactory;
import de.invesdwin.util.lang.Files;

/**
 * If you need to store large data on disk, it is better to use LevelDB only for an ordered index and store the actual
 * db in a file based persistent hash map. This is because LevelDB has very bad insertion speed when handling large
 * elements.
 */
@ThreadSafe
public class PersistentTreeMapDBFactory<K, V> implements IPersistentNavigableMapFactory<K, V> {

    @Override
    public ConcurrentNavigableMap<K, V> newPersistentNavigableMap(final IPersistentNavigableMapConfig<K, V> config) {
        final Maker fileDB = createDB(config);
        final DB db = configureDB(config, fileDB).make();
        final TreeMapMaker<K, V> maker = db.treeMap(config.getName(),
                new SerdeGroupSerializer<K>(config.newKeySerde(), config.newComparator()),
                new SerdeGroupSerializer<V>(config.newValueSerde()));
        return configureHashMap(config, maker).createOrOpen();
    }

    @Override
    public boolean isDiskPersistenceSupported() {
        return true;
    }

    protected Maker createDB(final IPersistentMapConfig<K, V> config) {
        final File file = new File(config.getFile(), "treemap.db");
        try {
            Files.forceMkdirParent(file);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        return DBMaker.fileDB(file);
    }

    protected Maker configureDB(final IPersistentMapConfig<K, V> config, final Maker maker) {
        return maker.fileMmapEnable().fileMmapPreclearDisable().cleanerHackEnable().closeOnJvmShutdownWeakReference();
    }

    protected TreeMapMaker<K, V> configureHashMap(final IPersistentMapConfig<K, V> config,
            final TreeMapMaker<K, V> maker) {
        return maker.counterEnable();
    }

    @Override
    public void removeAll(final Map<K, V> table, final IKeyMatcher<K> matcher) {
        final BTreeMap<K, V> cTable = (BTreeMap<K, V>) table;
        for (final K key : cTable.keySet()) {
            if (matcher.matches(key)) {
                cTable.remove(key);
            }
        }
    }

}
