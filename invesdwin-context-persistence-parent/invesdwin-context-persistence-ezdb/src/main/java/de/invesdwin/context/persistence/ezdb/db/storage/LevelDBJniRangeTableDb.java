package de.invesdwin.context.persistence.ezdb.db.storage;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;

import javax.annotation.concurrent.NotThreadSafe;

import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;

import de.invesdwin.context.persistence.ezdb.EzdbSerde;
import de.invesdwin.context.persistence.ezdb.db.IRangeTableDb;
import de.invesdwin.util.error.Throwables;
import ezdb.leveldb.EzLevelDbJni;
import ezdb.leveldb.EzLevelDbJniFactory;
import ezdb.table.Table;
import ezdb.table.range.RangeTable;

@NotThreadSafe
public class LevelDBJniRangeTableDb implements IRangeTableDb {

    private final RangeTableInternalMethods internalMethods;
    private final EzLevelDbJni db;

    public LevelDBJniRangeTableDb(final RangeTableInternalMethods internalMethods) {
        this.internalMethods = internalMethods;
        this.db = new EzLevelDbJni(internalMethods.getDirectory(), new EzLevelDbJniFactory() {
            @Override
            public DB open(final File path, final org.iq80.leveldb.Options options, final boolean rangeTable)
                    throws IOException {
                options.paranoidChecks(false);
                //make sure snappy is enabled
                options.compressionType(newCompressionType());
                final DB open = super.open(path, options, rangeTable);
                try {
                    //do some sanity checks just to be safe
                    try (DBIterator iterator = open.iterator()) {
                        iterator.seekToFirst();
                        if (iterator.hasNext()) {
                            final Entry<byte[], byte[]> next = iterator.next();
                            internalMethods.validateRowBytes(next, rangeTable);
                        }
                        iterator.seekToLast();
                        if (iterator.hasPrev()) {
                            final Entry<byte[], byte[]> prev = iterator.prev();
                            internalMethods.validateRowBytes(prev, rangeTable);
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
        return CompressionType.SNAPPY;
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
