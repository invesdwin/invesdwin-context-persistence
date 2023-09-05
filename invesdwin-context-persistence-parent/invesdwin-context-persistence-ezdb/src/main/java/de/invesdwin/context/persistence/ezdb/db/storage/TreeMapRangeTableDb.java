package de.invesdwin.context.persistence.ezdb.db.storage;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.ezdb.EzdbSerde;
import de.invesdwin.context.persistence.ezdb.db.IRangeTableDb;
import ezdb.table.Table;
import ezdb.table.range.RangeTable;
import ezdb.treemap.object.EzObjectTreeMapDb;

@Immutable
public class TreeMapRangeTableDb implements IRangeTableDb {

    private final RangeTableInternalMethods internalMethods;
    private final EzObjectTreeMapDb db;

    public TreeMapRangeTableDb(final RangeTableInternalMethods internalMethods) {
        this.internalMethods = internalMethods;
        this.db = newDb();
    }

    protected EzObjectTreeMapDb newDb() {
        return new EzObjectTreeMapDb();
    }

    @Override
    public <_H, _R, _V> RangeTable<_H, _R, _V> getRangeTable(final String tableName) {
        return db.getRangeTable(tableName, null, null, null, internalMethods.getHashKeyComparatorMemory(),
                internalMethods.getRangeKeyComparatorMemory());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <H, V> Table<H, V> getTable(final String tableName) {
        internalMethods.initDirectory();
        return db.getTable(tableName, EzdbSerde.valueOf(internalMethods.getHashKeySerde()),
                EzdbSerde.valueOf(internalMethods.getValueSerde()), internalMethods.getHashKeyComparatorMemory());
    }

    @Override
    public void deleteTable(final String tableName) {
        db.deleteTable(tableName);
    }

}
