package de.invesdwin.context.persistence.ezdb.db.storage;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import de.invesdwin.context.persistence.ezdb.EzdbSerde;
import de.invesdwin.context.persistence.ezdb.db.IRangeTableDb;
import de.invesdwin.util.error.Throwables;
import ezdb.RangeTable;
import ezdb.RawTableRow;
import ezdb.lmdb.EzLmDb;
import ezdb.lmdb.EzLmDbComparator;
import ezdb.lmdb.EzLmDbJnrFactory;
import ezdb.lmdb.util.LmDBJnrDBIterator;

@NotThreadSafe
public class LmdbRangeTableDb implements IRangeTableDb {

    private final RangeTableInternalMethods internalMethods;
    private final EzLmDb db;

    public LmdbRangeTableDb(final RangeTableInternalMethods internalMethods) {
        this.internalMethods = internalMethods;
        this.db = new EzLmDb(internalMethods.getDirectory(), new EzLmDbJnrFactory() {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            @Override
            public Dbi<java.nio.ByteBuffer> open(final String tableName, final Env<java.nio.ByteBuffer> env,
                    final EzLmDbComparator comparator, final DbiFlags... dbiFlags) throws IOException {
                final Dbi<java.nio.ByteBuffer> dbi = super.open(tableName, env, comparator, dbiFlags);
                try {
                    //do some sanity checks just to be safe
                    try (Txn<java.nio.ByteBuffer> txnRead = env.txnRead()) {
                        try (LmDBJnrDBIterator iterator = new LmDBJnrDBIterator(env, dbi,
                                EzdbSerde.valueOf(internalMethods.getHashKeySerde()),
                                EzdbSerde.valueOf(internalMethods.getRangeKeySerde()),
                                EzdbSerde.valueOf(internalMethods.getValueSerde()))) {
                            iterator.seekToFirst();
                            if (iterator.hasNext()) {
                                final RawTableRow next = iterator.next();
                                internalMethods.validateRow(next);
                            }
                            iterator.seekToLast();
                            if (iterator.hasPrev()) {
                                final RawTableRow prev = iterator.prev();
                                internalMethods.validateRow(prev);
                            }
                        }
                    }
                    return dbi;
                } catch (final Throwable t) {
                    dbi.close();
                    throw Throwables.propagate(t);
                }
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <_H, _R, _V> RangeTable<_H, _R, _V> getTable(final String tableName) {
        internalMethods.initDirectory();
        return db.getTable(tableName, EzdbSerde.valueOf(internalMethods.getHashKeySerde()),
                EzdbSerde.valueOf(internalMethods.getRangeKeySerde()),
                EzdbSerde.valueOf(internalMethods.getValueSerde()), internalMethods.getHashKeyComparatorDisk(),
                internalMethods.getRangeKeyComparatorDisk());
    }

    @Override
    public void deleteTable(final String tableName) {
        db.deleteTable(tableName);
    }

}
