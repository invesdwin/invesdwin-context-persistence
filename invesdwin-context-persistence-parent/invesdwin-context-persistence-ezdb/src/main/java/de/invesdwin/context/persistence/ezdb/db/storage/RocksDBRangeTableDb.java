package de.invesdwin.context.persistence.ezdb.db.storage;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import de.invesdwin.context.persistence.ezdb.EzdbSerde;
import de.invesdwin.context.persistence.ezdb.db.IRangeTableDb;
import de.invesdwin.util.bean.tuple.ImmutableEntry;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.error.Throwables;
import ezdb.rocksdb.EzRocksDb;
import ezdb.rocksdb.EzRocksDbJniFactory;
import ezdb.table.Table;
import ezdb.table.range.RangeTable;

@NotThreadSafe
public class RocksDBRangeTableDb implements IRangeTableDb {

    private final RangeTableInternalMethods internalMethods;
    private final EzRocksDb db;

    public RocksDBRangeTableDb(final RangeTableInternalMethods internalMethods) {
        this.internalMethods = internalMethods;
        this.db = new EzRocksDb(internalMethods.getDirectory(), new EzRocksDbJniFactory() {
            @Override
            public RocksDB open(final File path, final Options options, final boolean rangeTable) throws IOException {
                options.setCompressionType(newCompressionType());
                options.optimizeLevelStyleCompaction();
                options.setIncreaseParallelism(Executors.getCpuThreadPoolCount());
                final RocksDB open = super.open(path, options, rangeTable);
                try {
                    //do some sanity checks just to be safe
                    try (RocksIterator iterator = open.newIterator()) {
                        iterator.seekToFirst();
                        if (iterator.isValid()) {
                            final byte[] key = iterator.key();
                            final byte[] value = iterator.value();
                            internalMethods.validateRowBytes(ImmutableEntry.of(key, value), rangeTable);
                        }
                        iterator.seekToLast();
                        if (iterator.isValid()) {
                            final byte[] key = iterator.key();
                            final byte[] value = iterator.value();
                            internalMethods.validateRowBytes(ImmutableEntry.of(key, value), rangeTable);
                        }
                    }
                    return open;
                } catch (final Throwable t) {
                    open.close();
                    throw Throwables.propagate(t);
                }
            }

        });
    }

    protected CompressionType newCompressionType() {
        return CompressionType.SNAPPY_COMPRESSION;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <_H, _R, _V> RangeTable<_H, _R, _V> getRangeTable(final String tableName) {
        internalMethods.initDirectory();
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
