package de.invesdwin.context.persistence.ezdb;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.persistentmap.APersistentMapConfig;
import de.invesdwin.context.integration.persistentmap.IKeyMatcher;
import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;
import de.invesdwin.context.persistence.ezdb.table.ADelegateTable;
import de.invesdwin.context.persistence.ezdb.table.TableConcurrentMap;
import de.invesdwin.util.marshallers.serde.ISerde;
import ezdb.table.Batch;
import ezdb.table.TableRow;
import ezdb.util.TableIterator;

@Immutable
public class PersistentEzdbMapFactory<K, V> implements IPersistentMapFactory<K, V> {

    @Override
    public ConcurrentMap<K, V> newPersistentMap(final APersistentMapConfig<K, V> config) {
        final ADelegateTable<K, V> table = new ADelegateTable<K, V>(config.getName()) {

            @Override
            protected File getBaseDirectory() {
                return config.getBaseDirectory();
            }

            @Override
            protected File getDirectory() {
                return config.getDirectory();
            }

            @Override
            protected ISerde<K> newHashKeySerde() {
                return config.newKeySerde();
            }

            @Override
            protected ISerde<V> newValueSerde() {
                return config.newValueSerde();
            }

            @Override
            protected boolean allowHasNext() {
                return true;
            }
        };
        return new TableConcurrentMap<K, V>(table);
    }

    @Override
    public void removeAll(final ConcurrentMap<K, V> table, final IKeyMatcher<K> matcher) {
        final TableConcurrentMap<K, V> cTable = (TableConcurrentMap<K, V>) table;
        final TableIterator<? extends TableRow<K, V>> range = cTable.getTable().range();
        final Batch<K, V> batch = cTable.getTable().newBatch();
        try {
            try {
                while (true) {
                    final TableRow<K, V> next = range.next();
                    if (matcher.matches(next.getHashKey())) {
                        batch.delete(next.getKey());
                    }
                }
            } catch (final NoSuchElementException e) {
                //end reached
            }
            batch.flush();
        } finally {
            range.close();
            try {
                batch.close();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
