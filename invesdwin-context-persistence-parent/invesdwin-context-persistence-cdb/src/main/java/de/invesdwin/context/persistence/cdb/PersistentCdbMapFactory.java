package de.invesdwin.context.persistence.cdb;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.Immutable;

import com.strangegizmo.cdb.Cdb;

import de.invesdwin.context.integration.persistentmap.IKeyMatcher;
import de.invesdwin.context.integration.persistentmap.IPersistentMapConfig;
import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;
import de.invesdwin.util.lang.Files;

@Immutable
public class PersistentCdbMapFactory<K, V> implements IPersistentMapFactory<K, V> {

    @Override
    public ConcurrentMap<K, V> newPersistentMap(final IPersistentMapConfig<K, V> config) {
        try {
            Files.forceMkdirParent(config.getFile());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        final Cdb cdb;
        try {
            cdb = new Cdb(config.getFile().getAbsolutePath());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        return new CdbMap<>(config.getFile(), cdb, config.newKeySerde(), config.newValueSerde());
    }

    @Override
    public boolean isDiskPersistenceSupported() {
        return true;
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
