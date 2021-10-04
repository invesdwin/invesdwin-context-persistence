package de.invesdwin.context.persistence.ezdb;

import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.persistentmap.APersistentMapConfig;
import de.invesdwin.context.integration.persistentmap.IKeyMatcher;
import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;
import de.invesdwin.context.persistence.ezdb.table.TableConcurrentMap;
import de.invesdwin.context.persistence.ezdb.table.range.ADelegateRangeTable;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.VoidSerde;
import ezdb.table.TableRow;
import ezdb.util.TableIterator;

@Immutable
public class EzdbPersistentMapFactory<K, V> implements IPersistentMapFactory<K, V> {

    @Override
    public ConcurrentMap<K, V> newPersistentMap(final APersistentMapConfig<K, V> config) {
        final ADelegateRangeTable<K, Void, V> table = new ADelegateRangeTable<K, Void, V>(config.getName()) {
            @Override
            protected ISerde<K> newHashKeySerde() {
                return config.newKeySerde();
            }

            @Override
            protected ISerde<Void> newRangeKeySerde() {
                return VoidSerde.GET;
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
        try {
            while (true) {
                final TableRow<K, V> next = range.next();
                if (matcher.matches(next.getHashKey())) {
                    range.remove();
                }
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
    }

}
