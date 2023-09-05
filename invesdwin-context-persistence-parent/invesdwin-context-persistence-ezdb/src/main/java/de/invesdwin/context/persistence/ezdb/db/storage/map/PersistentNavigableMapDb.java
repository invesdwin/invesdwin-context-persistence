package de.invesdwin.context.persistence.ezdb.db.storage.map;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.persistentmap.navigable.IPersistentNavigableMapConfig;
import de.invesdwin.context.integration.persistentmap.navigable.IPersistentNavigableMapFactory;
import de.invesdwin.context.persistence.ezdb.db.storage.RangeTableInternalMethods;
import ezdb.Db;
import ezdb.comparator.LexicographicalComparator;
import ezdb.table.Table;
import ezdb.table.range.RangeTable;
import ezdb.treemap.bytes.table.BytesTreeMapTable;
import ezdb.treemap.bytes.table.range.BytesTreeMapRangeTable;

@ThreadSafe
public class PersistentNavigableMapDb implements Db<java.nio.ByteBuffer> {
    private final Map<String, Table<?, ?>> cache;
    private final RangeTableInternalMethods internalMethods;
    @SuppressWarnings("rawtypes")
    private final IPersistentNavigableMapFactory factory;

    public PersistentNavigableMapDb(final RangeTableInternalMethods internalMethods,
            final IPersistentNavigableMapFactory<?, ?> factory) {
        this.cache = new HashMap<String, Table<?, ?>>();
        this.internalMethods = internalMethods;
        this.factory = factory;
    }

    @Override
    public void deleteTable(final String tableName) {
        synchronized (cache) {
            cache.remove(tableName);
        }
    }

    @Override
    public <H, V> Table<H, V> getTable(final String tableName, final ezdb.serde.Serde<H> hashKeySerde,
            final ezdb.serde.Serde<V> valueSerde) {
        return getTable(tableName, hashKeySerde, valueSerde, new LexicographicalComparator());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <H, V> Table<H, V> getTable(final String tableName, final ezdb.serde.Serde<H> hashKeySerde,
            final ezdb.serde.Serde<V> valueSerde, final Comparator<java.nio.ByteBuffer> hashKeyComparator) {
        synchronized (cache) {
            Table<?, ?> table = cache.get(tableName);

            if (table == null) {
                table = newTable(tableName, hashKeySerde, valueSerde, hashKeyComparator);
                cache.put(tableName, table);
            }

            return (Table<H, V>) table;
        }
    }

    @Override
    public <H, R, V> RangeTable<H, R, V> getRangeTable(final String tableName, final ezdb.serde.Serde<H> hashKeySerde,
            final ezdb.serde.Serde<R> rangeKeySerde, final ezdb.serde.Serde<V> valueSerde) {
        return getRangeTable(tableName, hashKeySerde, rangeKeySerde, valueSerde, new LexicographicalComparator(),
                new LexicographicalComparator());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <H, R, V> RangeTable<H, R, V> getRangeTable(final String tableName, final ezdb.serde.Serde<H> hashKeySerde,
            final ezdb.serde.Serde<R> rangeKeySerde, final ezdb.serde.Serde<V> valueSerde,
            final Comparator<java.nio.ByteBuffer> hashKeyComparator,
            final Comparator<java.nio.ByteBuffer> rangeKeyComparator) {
        synchronized (cache) {
            RangeTable<?, ?, ?> table = (RangeTable<?, ?, ?>) cache.get(tableName);

            if (table == null) {
                table = newRangeTable(tableName, hashKeySerde, rangeKeySerde, valueSerde, hashKeyComparator,
                        rangeKeyComparator);
                cache.put(tableName, table);
            }

            return (RangeTable<H, R, V>) table;
        }
    }

    protected <H, R, V> BytesTreeMapRangeTable<H, R, V> newRangeTable(final String tableName,
            final ezdb.serde.Serde<H> hashKeySerde, final ezdb.serde.Serde<R> rangeKeySerde,
            final ezdb.serde.Serde<V> valueSerde, final Comparator<java.nio.ByteBuffer> hashKeyComparator,
            final Comparator<java.nio.ByteBuffer> rangeKeyComparator) {
        return new BytesTreeMapRangeTable<H, R, V>(hashKeySerde, rangeKeySerde, valueSerde, hashKeyComparator,
                rangeKeyComparator) {
            @SuppressWarnings("unchecked")
            @Override
            protected NavigableMap<java.nio.ByteBuffer, java.nio.ByteBuffer> newMap(
                    final Comparator<java.nio.ByteBuffer> comparator) {
                final IPersistentNavigableMapConfig<java.nio.ByteBuffer, java.nio.ByteBuffer> config = new PersistentNavigableMapDbConfig(
                        tableName, factory, internalMethods.getBaseDirectory(), comparator);
                return factory.newPersistentNavigableMap(config);
            }
        };
    }

    protected <H, V> BytesTreeMapTable<H, V> newTable(final String tableName, final ezdb.serde.Serde<H> hashKeySerde,
            final ezdb.serde.Serde<V> valueSerde, final Comparator<java.nio.ByteBuffer> hashKeyComparator) {
        return new BytesTreeMapTable<H, V>(hashKeySerde, valueSerde, hashKeyComparator) {
            @SuppressWarnings("unchecked")
            @Override
            protected Map<java.nio.ByteBuffer, java.nio.ByteBuffer> newMap(
                    final Comparator<java.nio.ByteBuffer> comparator) {
                final IPersistentNavigableMapConfig<java.nio.ByteBuffer, java.nio.ByteBuffer> config = new PersistentNavigableMapDbConfig(
                        tableName, factory, internalMethods.getBaseDirectory(), comparator);
                return factory.newPersistentNavigableMap(config);
            }
        };
    }

}
