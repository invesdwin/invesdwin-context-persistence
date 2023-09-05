package de.invesdwin.context.persistence.ezdb.db.storage;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;

import javax.annotation.concurrent.NotThreadSafe;

import com.indeed.lsmtree.core.Store;
import com.indeed.lsmtree.core.Store.Entry;
import com.indeed.util.serialization.Serializer;

import de.invesdwin.context.persistence.ezdb.EzdbSerde;
import de.invesdwin.context.persistence.ezdb.db.IRangeTableDb;
import ezdb.lsmtree.EzLsmTreeDb;
import ezdb.lsmtree.EzLsmTreeDbJavaFactory;
import ezdb.table.Table;
import ezdb.table.range.RangeTable;
import ezdb.util.ObjectRangeTableKey;

@NotThreadSafe
public class LsmTreeRangeTableDb implements IRangeTableDb {

    private final RangeTableInternalMethods internalMethods;
    private final EzLsmTreeDb db;

    public LsmTreeRangeTableDb(final RangeTableInternalMethods internalMethods) {
        this.internalMethods = internalMethods;
        this.db = new EzLsmTreeDb(internalMethods.getDirectory(), new EzLsmTreeDbJavaFactory() {

            @Override
            public <H, V> Store<H, V> open(final File path, final Serializer<H> keySerializer,
                    final Serializer<V> valueSerializer, final Comparator<H> comparator) throws IOException {
                final Store<H, V> store = super.open(path, keySerializer, valueSerializer, comparator);
                final Entry<H, V> first = store.first();
                validateRow(first);
                final Entry<H, V> last = store.last();
                validateRow(last);
                return store;
            }

        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <_H, _R, _V> RangeTable<_H, _R, _V> getRangeTable(final String tableName) {
        internalMethods.initDirectory();
        return db.getRangeTable(tableName, EzdbSerde.valueOf(internalMethods.getHashKeySerde()),
                EzdbSerde.valueOf(internalMethods.getRangeKeySerde()),
                EzdbSerde.valueOf(internalMethods.getValueSerde()), internalMethods.getHashKeyComparatorMemory(),
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

    public static <H, V> void validateRow(final Entry<H, V> row) {
        if (row != null) {
            final H key = row.getKey();
            if (key instanceof ObjectRangeTableKey) {
                final ObjectRangeTableKey cKey = (ObjectRangeTableKey) key;
                cKey.getHashKey();
                cKey.getRangeKey();
            }
            row.getValue();
        }
    }

}
