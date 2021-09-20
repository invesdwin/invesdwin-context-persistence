package de.invesdwin.context.persistence.ezdb.db.storage;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.indeed.lsmtree.core.Store;
import com.indeed.lsmtree.core.Store.Entry;
import com.indeed.util.serialization.Serializer;

import de.invesdwin.context.persistence.ezdb.EzdbSerde;
import de.invesdwin.context.persistence.ezdb.db.IRangeTableDb;
import ezdb.RangeTable;
import ezdb.lsmtree.EzLsmTreeDb;
import ezdb.lsmtree.EzLsmTreeDbComparator;
import ezdb.lsmtree.EzLsmTreeDbJavaFactory;
import ezdb.util.ObjectTableKey;

@NotThreadSafe
public class LsmTreeRangeTableDb implements IRangeTableDb {

    private final RangeTableInternalMethods internalMethods;
    private final EzLsmTreeDb db;

    public LsmTreeRangeTableDb(final RangeTableInternalMethods internalMethods) {
        this.internalMethods = internalMethods;
        this.db = new EzLsmTreeDb(internalMethods.getDirectory(), new EzLsmTreeDbJavaFactory() {

            @Override
            public <H, R, V> Store<ObjectTableKey<H, R>, V> open(final File path,
                    final Serializer<ObjectTableKey<H, R>> keySerializer, final Serializer<V> valueSerializer,
                    final EzLsmTreeDbComparator<H, R> comparator) throws IOException {
                final Store<ObjectTableKey<H, R>, V> store = super.open(path, keySerializer, valueSerializer,
                        comparator);
                final Entry<ObjectTableKey<H, R>, V> first = store.first();
                validateRow(first);
                final Entry<ObjectTableKey<H, R>, V> last = store.last();
                validateRow(last);
                return store;
            }

        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <_H, _R, _V> RangeTable<_H, _R, _V> getTable(final String tableName) {
        internalMethods.initDirectory();
        return db.getTable(tableName, EzdbSerde.valueOf(internalMethods.getHashKeySerde()),
                EzdbSerde.valueOf(internalMethods.getRangeKeySerde()),
                EzdbSerde.valueOf(internalMethods.getValueSerde()), internalMethods.getHashKeyComparatorMemory(),
                internalMethods.getRangeKeyComparatorMemory());
    }

    @Override
    public void deleteTable(final String tableName) {
        db.deleteTable(tableName);
    }

    public static <H, R, V> void validateRow(final Entry<ObjectTableKey<H, R>, V> row) {
        if (row != null) {
            row.getKey().getHashKey();
            row.getKey().getRangeKey();
            row.getValue();
        }
    }

}
