package de.invesdwin.context.persistence.tkrzw;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.persistentmap.APersistentMapConfig;
import de.invesdwin.context.integration.persistentmap.IKeyMatcher;
import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;
import de.invesdwin.instrument.DynamicInstrumentationReflections;
import de.invesdwin.util.lang.Files;
import tkrzw.DBM;
import tkrzw.Status;

@Immutable
public class PersistentTkrzwMapFactory<K, V> implements IPersistentMapFactory<K, V> {

    static {
        for (final String path : TkrzwProperties.TKRZW_LIBRARY_PATHS) {
            DynamicInstrumentationReflections.addPathToJavaLibraryPath(new File(path));
        }
    }

    @Override
    public ConcurrentMap<K, V> newPersistentMap(final APersistentMapConfig<K, V> config) {
        final DBM dbm = new DBM();
        try {
            Files.forceMkdirParent(config.getFile());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        final Status status = openDbm(config, dbm);
        if (status.getCode() != Status.SUCCESS) {
            throw new IllegalStateException(status.toString());
        }
        return new TkrzwMap<>(dbm, config.newKeySerde(), config.newValueSerde());
    }

    protected Status openDbm(final APersistentMapConfig<K, V> config, final DBM dbm) {
        return dbm.open(config.getFile().getAbsolutePath(), true);
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
