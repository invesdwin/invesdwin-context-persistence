package de.invesdwin.context.persistence.timeseries.ezdb.db.type;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.timeseries.ezdb.db.IRangeTableDb;
import ezdb.RangeTable;
import ezdb.treemap.object.EzObjectTreeMapDb;

@Immutable
public class TreeMapRangeTableDb implements IRangeTableDb {

    private final RangeTableInternalMethods internalMethods;
    private final EzObjectTreeMapDb db;

    public TreeMapRangeTableDb(final RangeTableInternalMethods internalMethods) {
        this.internalMethods = internalMethods;
        this.db = new EzObjectTreeMapDb();
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
