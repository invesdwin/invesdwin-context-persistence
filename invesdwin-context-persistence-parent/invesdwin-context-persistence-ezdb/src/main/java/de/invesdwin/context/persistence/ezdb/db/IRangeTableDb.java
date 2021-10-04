package de.invesdwin.context.persistence.ezdb.db;

import ezdb.table.Table;
import ezdb.table.range.RangeTable;

public interface IRangeTableDb {

    <H, R, V> RangeTable<H, R, V> getRangeTable(String tableName);

    <H, V> Table<H, V> getTable(String tableName);

    void deleteTable(String tableName);

}
