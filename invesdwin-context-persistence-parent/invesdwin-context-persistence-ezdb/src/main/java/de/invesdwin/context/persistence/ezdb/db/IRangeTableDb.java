package de.invesdwin.context.persistence.ezdb.db;

import ezdb.RangeTable;

public interface IRangeTableDb {

    <H, R, V> RangeTable<H, R, V> getTable(String tableName);

    void deleteTable(String tableName);

}
