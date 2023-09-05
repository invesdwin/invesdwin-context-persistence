package de.invesdwin.context.persistence.ezdb.db.storage.map;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.persistentmap.navigable.IPersistentNavigableMapFactory;
import de.invesdwin.context.persistence.ezdb.EzdbSerde;
import de.invesdwin.context.persistence.ezdb.db.IRangeTableDb;
import de.invesdwin.context.persistence.ezdb.db.storage.RangeTableInternalMethods;
import ezdb.table.Table;
import ezdb.table.range.RangeTable;

@Immutable
public class PersistentNavigableMapRangeTableDb implements IRangeTableDb {

    private final RangeTableInternalMethods internalMethods;
    private final IPersistentNavigableMapFactory<?, ?> factory;
    private final PersistentNavigableMapDb db;

    public PersistentNavigableMapRangeTableDb(final RangeTableInternalMethods internalMethods,
            final IPersistentNavigableMapFactory<?, ?> factory) {
        this.internalMethods = internalMethods;
        this.factory = factory;
        this.db = newDb();
    }

    protected PersistentNavigableMapDb newDb() {
        return new PersistentNavigableMapDb(internalMethods, factory);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <_H, _R, _V> RangeTable<_H, _R, _V> getRangeTable(final String tableName) {
        return db.getRangeTable(tableName, EzdbSerde.valueOf(internalMethods.getHashKeySerde()),
                EzdbSerde.valueOf(internalMethods.getRangeKeySerde()),
                EzdbSerde.valueOf(internalMethods.getValueSerde()), internalMethods.getHashKeyComparatorDisk(),
                internalMethods.getRangeKeyComparatorDisk());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <H, V> Table<H, V> getTable(final String tableName) {
        internalMethods.initDirectory();
        return db.getTable(tableName, EzdbSerde.valueOf(internalMethods.getHashKeySerde()),
                EzdbSerde.valueOf(internalMethods.getValueSerde()), internalMethods.getHashKeyComparatorDisk());
    }

    @Override
    public void deleteTable(final String tableName) {
        db.deleteTable(tableName);
    }

}
