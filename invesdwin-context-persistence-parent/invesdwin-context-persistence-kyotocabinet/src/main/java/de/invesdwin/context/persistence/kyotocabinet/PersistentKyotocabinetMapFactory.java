package de.invesdwin.context.persistence.kyotocabinet;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.persistentmap.IKeyMatcher;
import de.invesdwin.context.integration.persistentmap.IPersistentMapConfig;
import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;
import de.invesdwin.instrument.DynamicInstrumentationReflections;
import de.invesdwin.util.lang.Files;
import kyotocabinet.DB;

@Immutable
public class PersistentKyotocabinetMapFactory<K, V> implements IPersistentMapFactory<K, V> {

    static {
        for (final String path : KyotocabinetProperties.KYOTOCABINET_LIBRARY_PATHS) {
            DynamicInstrumentationReflections.addPathToJavaLibraryPath(new File(path));
        }
    }

    @Override
    public ConcurrentMap<K, V> newPersistentMap(final IPersistentMapConfig<K, V> config) {
        final DB dbm = new DB();
        try {
            Files.forceMkdirParent(config.getFile());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        final boolean status = openDb(config, dbm);
        if (!status) {
            throw new IllegalStateException("could not open db " + dbm.error());
        }
        return new KyotocabinetMap<>(dbm, config.newKeySerde(), config.newValueSerde());
    }

    @Override
    public boolean isDiskPersistenceSupported() {
        return true;
    }

    protected boolean openDb(final IPersistentMapConfig<K, V> config, final DB db) {
        return db.open(config.getFile().getAbsolutePath(), DB.OWRITER | DB.OCREATE);
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
