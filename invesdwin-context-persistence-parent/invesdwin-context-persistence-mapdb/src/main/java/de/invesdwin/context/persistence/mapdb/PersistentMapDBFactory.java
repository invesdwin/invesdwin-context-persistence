package de.invesdwin.context.persistence.mapdb;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.ThreadSafe;

import org.mapdb.DB;
import org.mapdb.DB.HashMapMaker;
import org.mapdb.DBMaker;
import org.mapdb.DBMaker.Maker;
import org.mapdb.HTreeMap;

import de.invesdwin.context.integration.persistentmap.APersistentMapConfig;
import de.invesdwin.context.integration.persistentmap.IKeyMatcher;
import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;
import de.invesdwin.util.lang.Files;

/**
 * If you need to store large data on disk, it is better to use LevelDB only for an ordered index and store the actual
 * db in a file based persistent hash map. This is because LevelDB has very bad insertion speed when handling large
 * elements.
 */
@ThreadSafe
public class PersistentMapDBFactory<K, V> implements IPersistentMapFactory<K, V> {

    @Override
    public ConcurrentMap<K, V> newPersistentMap(final APersistentMapConfig<K, V> config) {
        final Maker fileDB = createDB(config);
        final DB db = configureDB(config, fileDB).make();
        final HashMapMaker<K, V> maker = db.hashMap(config.getName(), new SerdeGroupSerializer<K>(config.newKeySerde()),
                new SerdeGroupSerializer<V>(config.newValueSerde()));
        return configureHashMap(config, maker).createOrOpen();
    }

    protected Maker createDB(final APersistentMapConfig<K, V> config) {
        final File file = config.getFile();
        try {
            Files.forceMkdirParent(file);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        return DBMaker.fileDB(file);
    }

    protected Maker configureDB(final APersistentMapConfig<K, V> config, final Maker maker) {
        return maker.fileMmapEnable()
                .fileMmapPreclearDisable()
                .closeOnJvmShutdownWeakReference()
                .cleanerHackEnable()
                .checksumHeaderBypass();
    }

    protected HashMapMaker<K, V> configureHashMap(final APersistentMapConfig<K, V> config,
            final HashMapMaker<K, V> maker) {
        return maker.counterEnable();
    }

    @Override
    public void removeAll(final ConcurrentMap<K, V> table, final IKeyMatcher<K> matcher) {
        final HTreeMap<K, V> cTable = (HTreeMap<K, V>) table;
        for (final K key : cTable.keySet()) {
            if (matcher.matches(key)) {
                cTable.remove(key);
            }
        }
    }

}
