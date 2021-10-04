package de.invesdwin.context.persistence.ezdb.db;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import ezdb.table.Batch;
import ezdb.table.Table;
import ezdb.table.TableRow;
import ezdb.util.TableIterator;

@NotThreadSafe
public class WriteThorughTable<H, V> implements Table<H, V> {

    private final Table<H, V> memory;
    private final Table<H, V> disk;

    public WriteThorughTable(final Table<H, V> memory, final Table<H, V> disk) {
        this.memory = memory;
        this.disk = disk;
        try (TableIterator<? extends TableRow<H, V>> range = disk.range()) {
            final TableRow<H, V> next = range.next();
            memory.put(next.getHashKey(), next.getValue());
        }
    }

    @Override
    public void put(final H hashKey, final V value) {
        memory.put(hashKey, value);
        disk.put(hashKey, value);
    }

    @Override
    public V get(final H hashKey) {
        return memory.get(hashKey);
    }

    @Override
    public void delete(final H hashKey) {
        memory.delete(hashKey);
        disk.delete(hashKey);
    }

    @Override
    public void close() {
        disk.close();
        memory.close();
    }

    @Override
    public TableIterator<? extends TableRow<H, V>> range() {
        return memory.range();
    }

    @Override
    public Batch<H, V> newBatch() {
        return new Batch<H, V>() {

            private final Batch<H, V> memoryBatch = memory.newBatch();
            private final Batch<H, V> diskBatch = disk.newBatch();

            @Override
            public void put(final H hashKey, final V value) {
                memoryBatch.put(hashKey, value);
                diskBatch.put(hashKey, value);
            }

            @Override
            public void delete(final H hashKey) {
                memoryBatch.delete(hashKey);
                diskBatch.delete(hashKey);
            }

            @Override
            public void flush() {
                memoryBatch.flush();
                diskBatch.flush();
            }

            @Override
            public void close() throws IOException {
                memoryBatch.close();
                diskBatch.close();
            }

        };
    }

}
