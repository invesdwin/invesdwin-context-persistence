package de.invesdwin.context.persistence.countdb;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.Immutable;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.application.EmbeddedDBContextFactory;
import be.bagofwords.db.combinator.OverWriteCombinator;
import de.invesdwin.context.integration.persistentmap.IKeyMatcher;
import de.invesdwin.context.integration.persistentmap.IPersistentMapConfig;
import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;
import de.invesdwin.util.lang.Files;

@Immutable
public class PersistentCountdbMapFactory<V> implements IPersistentMapFactory<Long, V> {

    @Override
    public ConcurrentMap<Long, V> newPersistentMap(final IPersistentMapConfig<Long, V> config) {
        try {
            Files.forceMkdirParent(config.getFile());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        final DataInterfaceFactory factory = EmbeddedDBContextFactory
                .createDataInterfaceFactory(config.getDirectory().getAbsolutePath());
        final DataInterface<V> dataInterface = factory.createDataInterface(config.getName(), config.getValueType(),
                new OverWriteCombinator<>(), new SerdeObjectSerializer<>(config.newValueSerde()));
        return new CountdbMap<V>(dataInterface);
    }

    @Override
    public void removeAll(final ConcurrentMap<Long, V> table, final IKeyMatcher<Long> matcher) {
        for (final Entry<Long, V> e : table.entrySet()) {
            final Long key = e.getKey();
            if (matcher.matches(key)) {
                table.remove(key);
            }
        }
    }

}
