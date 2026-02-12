package de.invesdwin.context.persistence.tokyocabinet;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.persistentmap.IKeyMatcher;
import de.invesdwin.context.integration.persistentmap.IPersistentMapConfig;
import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;
import de.invesdwin.instrument.DynamicInstrumentationReflections;
import de.invesdwin.util.lang.Files;
import tokyocabinet.HDB;

@Immutable
public class PersistentTokyocabinetMapFactory<K, V> implements IPersistentMapFactory<K, V> {

    static {
        for (final String path : TokyocabinetProperties.TOKYOCABINET_LIBRARY_PATHS) {
            DynamicInstrumentationReflections.addPathToJavaLibraryPath(new File(path));
        }
    }

    @Override
    public ConcurrentMap<K, V> newPersistentMap(final IPersistentMapConfig<K, V> config) {
        final HDB dbm = new HDB();
        try {
            Files.forceMkdirParent(config.getFile());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        final boolean status = openDbm(config, dbm);
        if (!status) {
            throw new IllegalStateException("could not open db " + dbm.errmsg());
        }
        return new TokyocabinetMap<>(dbm, config.newKeySerde(), config.newValueSerde());
    }

    @Override
    public boolean isDiskPersistenceSupported() {
        return true;
    }

    protected boolean openDbm(final IPersistentMapConfig<K, V> config, final HDB dbm) {
        return dbm.open(config.getFile().getAbsolutePath(), HDB.OWRITER | HDB.OCREAT);
    }

    @Override
    public void removeAll(final Map<K, V> table, final IKeyMatcher<K> matcher) {
        for (final Entry<K, V> e : table.entrySet()) {
            final K key = e.getKey();
            if (matcher.matches(key)) {
                table.remove(key);
            }
        }
    }

}
