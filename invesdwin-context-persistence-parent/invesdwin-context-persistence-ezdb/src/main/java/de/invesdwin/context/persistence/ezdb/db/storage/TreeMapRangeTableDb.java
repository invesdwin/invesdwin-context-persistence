package de.invesdwin.context.persistence.ezdb.db.storage;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.ezdb.db.IRangeTableDb;
import ezdb.RangeTable;
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
    public <_H, _R, _V> RangeTable<_H, _R, _V> getTable(final String tableName) {
        return db.getTable(tableName, null, null, null, internalMethods.getHashKeyComparatorMemory(),
                internalMethods.getRangeKeyComparatorMemory());
    }

    @Override
    public void deleteTable(final String tableName) {
        db.deleteTable(tableName);
    }

}
