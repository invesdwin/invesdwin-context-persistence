package de.invesdwin.context.persistence.ezdb.db;

import javax.annotation.concurrent.ThreadSafe;

import ezdb.table.Table;
import ezdb.table.range.RangeTable;

@ThreadSafe
public class WriteThroughRangeTableDb implements IRangeTableDb {

    private final IRangeTableDb memory;
    private final IRangeTableDb disk;

    public WriteThroughRangeTableDb(final IRangeTableDb memory, final IRangeTableDb disk) {
        this.memory = memory;
        this.disk = disk;
    }

    @Override
    public <H, R, V> RangeTable<H, R, V> getRangeTable(final String tableName) {
        final RangeTable<H, R, V> memoryTable = memory.getRangeTable(tableName);
        final RangeTable<H, R, V> diskTable = disk.getRangeTable(tableName);
        return new WriteThorughRangeTable<>(memoryTable, diskTable);
    }

    @Override
    public <H, V> Table<H, V> getTable(final String tableName) {
        final Table<H, V> memoryTable = memory.getTable(tableName);
        final Table<H, V> diskTable = disk.getTable(tableName);
        return new WriteThorughTable<>(memoryTable, diskTable);
    }

    @Override
    public void deleteTable(final String tableName) {
        memory.deleteTable(tableName);
        disk.deleteTable(tableName);
    }

}
