package de.invesdwin.context.persistence.leveldb.ezdb;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.persistence.leveldb.serde.ExtendedTypeDelegateSerde;
import de.invesdwin.util.bean.tuple.Pair;
import de.invesdwin.util.collections.iterable.ACloseableIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Reflections;
import de.invesdwin.util.lang.Strings;
import de.invesdwin.util.time.fdate.FDate;
import ezdb.Db;
import ezdb.RangeTable;
import ezdb.RawTableRow;
import ezdb.Table;
import ezdb.TableIterator;
import ezdb.TableRow;
import ezdb.batch.Batch;
import ezdb.batch.RangeBatch;
import ezdb.comparator.LexicographicalComparator;
import ezdb.comparator.SerdeComparator;
import ezdb.leveldb.EzLevelDb;
import ezdb.leveldb.EzLevelDbJniFactory;
import ezdb.serde.Serde;

@ThreadSafe
public abstract class ADelegateRangeTable<H, R, V> implements RangeTable<H, R, V> {

    private final Serde<H> hashKeySerde;
    private final Serde<R> rangeKeySerde;
    private final Serde<V> valueSerde;
    private final Comparator<byte[]> hashKeyComparator;
    private final Comparator<byte[]> rangeKeyComparator;

    private final Db db;
    private final ReadWriteLock tableLock = new ReentrantReadWriteLock();
    private final String name;
    private final File directory;
    private final File timestampFile;
    /**
     * used against too often accessing the timestampFile
     */
    private volatile FDate tableCreationTime;
    private volatile RangeTable<H, R, V> table;

    public ADelegateRangeTable(final String name) {
        this.name = name;
        this.directory = getDirectory();
        this.timestampFile = new File(new File(directory, getName()), "createdTimestamp");

        this.hashKeySerde = newHashKeySerde();
        this.rangeKeySerde = newRangeKeySerde();
        this.valueSerde = newValueSerde();
        this.hashKeyComparator = newHashKeyComparator();
        this.rangeKeyComparator = newRangeKeyComparator();
        this.db = initDB();
    }

    protected File getDirectory() {
        return new File(getBaseDirectory(), ADelegateRangeTable.class.getSimpleName());
    }

    protected File getBaseDirectory() {
        return ContextProperties.getHomeDirectory();
    }

    /**
     * Default is disabling hasNext for improved performance.
     */
    protected boolean allowHasNext() {
        return false;
    }

    protected boolean allowPutWithoutBatch() {
        return false;
    }

    protected Comparator<byte[]> newHashKeyComparator() {
        //order is not so important on the hashkey, so use bytes only
        return new LexicographicalComparator();
    }

    protected Comparator<byte[]> newRangeKeyComparator() {
        return new SerdeComparator<R>(rangeKeySerde);
    }

    @SuppressWarnings("unchecked")
    protected Serde<V> newValueSerde() {
        final Class<V> type = (Class<V>) Reflections.resolveTypeArguments(getClass(), ADelegateRangeTable.class)[2];
        return new ExtendedTypeDelegateSerde<V>(type);
    }

    @SuppressWarnings("unchecked")
    protected Serde<R> newRangeKeySerde() {
        final Class<R> type = (Class<R>) Reflections.resolveTypeArguments(getClass(), ADelegateRangeTable.class)[1];
        return new ExtendedTypeDelegateSerde<R>(type);
    }

    @SuppressWarnings("unchecked")
    protected Serde<H> newHashKeySerde() {
        final Class<H> type = (Class<H>) Reflections.resolveTypeArguments(getClass(), ADelegateRangeTable.class)[0];
        return new ExtendedTypeDelegateSerde<H>(type);
    }

    public String getName() {
        return name;
    }

    public FDate getTableCreationTime() {
        if (tableCreationTime == null) {
            if (!timestampFile.exists()) {
                return null;
            } else {
                tableCreationTime = FDate.valueOf(timestampFile.lastModified());
            }
        }
        return tableCreationTime;
    }

    private Db initDB() {
        initDirectory();
        return new EzLevelDb(directory, new EzLevelDbJniFactory() {
            @Override
            public DB open(final File path, final org.iq80.leveldb.Options options) throws IOException {
                options.verifyChecksums(false);
                options.paranoidChecks(false);
                final DB open = super.open(path, options);
                try {
                    //do some sanity checks just to be safe
                    try (DBIterator iterator = open.iterator()) {
                        iterator.seekToFirst();
                        if (iterator.hasNext()) {
                            final Entry<byte[], byte[]> next = iterator.next();
                            validateRow(next);
                        }
                        iterator.seekToLast();
                        if (iterator.hasPrev()) {
                            final Entry<byte[], byte[]> prev = iterator.prev();
                            validateRow(prev);
                        }
                    }
                    return open;
                } catch (final Throwable t) {
                    open.close();
                    throw Throwables.propagate(t);
                }
            }

            private void validateRow(final Entry<byte[], byte[]> rawRow) {
                //fst library might have been updated, in that case deserialization might fail
                final RawTableRow<H, R, V> row = new RawTableRow<H, R, V>(rawRow, hashKeySerde, rangeKeySerde,
                        valueSerde);
                row.getHashKey();
                row.getRangeKey();
                row.getValue();
            }
        });
    }

    private void initDirectory() {
        try {
            FileUtils.forceMkdir(directory);
        } catch (final IOException e) {
            throw Err.process(e);
        }
    }

    private RangeTable<H, R, V> getTableWithReadLock() {
        if (shouldPurgeTable()) {
            //only purge if currently not used
            if (tableLock.writeLock().tryLock()) {
                try {
                    //condition could have changed since lock has been acquired
                    if (shouldPurgeTable()) {
                        innerDeleteTable();
                    }
                } finally {
                    tableLock.writeLock().unlock();
                }
            }
        }

        //directly return table with read lock if not null
        tableLock.readLock().lock();
        if (table != null) {
            return table;
        }
        tableLock.readLock().unlock();

        //otherwise initialize it with write lock (thouch check again because of lock switch)
        initializeTable();

        //and return the now not null table with read lock
        tableLock.readLock().lock();
        if (table == null) {
            tableLock.readLock().unlock();
            throw new IllegalStateException("table should not be null here");
        }
        return table;
    }

    private void initializeTable() {
        tableLock.writeLock().lock();
        try {
            if (table == null) {
                if (getTableCreationTime() == null) {
                    try {
                        FileUtils.touch(timestampFile);
                    } catch (final IOException e) {
                        throw Err.process(e);
                    }
                }
                try {
                    table = db.getTable(name, hashKeySerde, rangeKeySerde, valueSerde, hashKeyComparator,
                            rangeKeyComparator);
                } catch (final Throwable e) {
                    if (Strings.containsIgnoreCase(e.getMessage(), "LOCK")) {
                        //ezdb.DbException: org.fusesource.leveldbjni.internal.NativeDB$DBException: IO error: lock /home/subes/Dokumente/Entwicklung/invesdwin/invesdwin-trading/invesdwin-trading-parent/invesdwin-trading-modules/invesdwin-trading-backtest/.invesdwin/de.invesdwin.context.persistence.leveldb.ADelegateRangeTable/CachingFinancialdataService_getInstrument/LOCK: Die Ressource ist zur Zeit nicht verfügbar
                        //at ezdb.leveldb.EzLevelDbTable.<init>(EzLevelDbTable.java:50)
                        //at ezdb.leveldb.EzLevelDb.getTable(EzLevelDb.java:69)
                        //at de.invesdwin.context.persistence.leveldb.ADelegateRangeTable.getTableWithReadLock(ADelegateRangeTable.java:144)
                        throw new RetryLaterRuntimeException(e);
                    } else {
                        Err.process(new RuntimeException("Table data for [" + getDirectory() + "/" + getName()
                                + "] is inconsistent. Resetting data and trying again.", e));
                        innerDeleteTable();
                        table = db.getTable(name, hashKeySerde, rangeKeySerde, valueSerde, hashKeyComparator,
                                rangeKeyComparator);
                    }
                }
            }
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    public void deleteTable() {
        tableLock.writeLock().lock();
        try {
            innerDeleteTable();
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    private void innerDeleteTable() {
        if (table != null) {
            table.close();
            table = null;
        }
        db.deleteTable(name);
        FileUtils.deleteQuietly(timestampFile);
        final File tableDirectory = new File(directory, getName());
        final String[] list = tableDirectory.list();
        if (list == null || list.length == 0) {
            FileUtils.deleteQuietly(tableDirectory);
        }
        tableCreationTime = null;
        onDeleteTableFinished();
    }

    protected void onDeleteTableFinished() {}

    protected boolean shouldPurgeTable() {
        return false;
    }

    @Override
    public void put(final H hashKey, final V value) {
        assertAllowedWriteWithoutBatch();
        final RangeTable<H, R, V> table = getTableWithReadLock();
        try {
            table.put(hashKey, value);
        } finally {
            tableLock.readLock().unlock();
        }
    }

    @Override
    public V get(final H hashKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock();
        try {
            return table.get(hashKey);
        } finally {
            tableLock.readLock().unlock();
        }
    }

    @Override
    public TableRow<H, R, V> getLatest(final H hashKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock();
        try {
            return table.getLatest(hashKey);
        } finally {
            tableLock.readLock().unlock();
        }
    }

    @Override
    public TableRow<H, R, V> getLatest(final H hashKey, final R rangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock();
        try {
            return table.getLatest(hashKey, rangeKey);
        } finally {
            tableLock.readLock().unlock();
        }
    }

    public V getLatestValue(final H hashKey, final R rangeKey) {
        return getValue(getLatest(hashKey, rangeKey));
    }

    private V getValue(final TableRow<H, R, V> row) {
        if (row == null) {
            return null;
        } else {
            return row.getValue();
        }
    }

    @Override
    public TableRow<H, R, V> getNext(final H hashKey, final R rangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock();
        try {
            return table.getNext(hashKey, rangeKey);
        } finally {
            tableLock.readLock().unlock();
        }
    }

    public R getNextRangeKey(final H hashKey, final R rangeKey) {
        return getRangeKey(getNext(hashKey, rangeKey));
    }

    public R getLatestRangeKey(final H hashKey, final R rangeKey) {
        return getRangeKey(getLatest(hashKey, rangeKey));
    }

    private R getRangeKey(final TableRow<H, R, V> row) {
        if (row == null) {
            return null;
        } else {
            return row.getRangeKey();
        }
    }

    @Override
    public TableRow<H, R, V> getPrev(final H hashKey, final R rangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock();
        try {
            return table.getPrev(hashKey, rangeKey);
        } finally {
            tableLock.readLock().unlock();
        }
    }

    public V getOrLoad(final H hashKey, final Function<H, V> loadable) {
        final Table<H, V> table = getTableWithReadLock();
        try {
            final V cachedValue = table.get(hashKey);
            if (cachedValue == null) {
                final V loadedValue = loadable.apply(hashKey);
                if (loadedValue != null) {
                    table.put(hashKey, loadedValue);
                }
                return loadedValue;
            } else {
                return cachedValue;
            }
        } catch (final Throwable t) {
            throw new RuntimeException("Key: " + hashKey, t);
        } finally {
            tableLock.readLock().unlock();
        }
    }

    public V getOrLoad(final H hashKey, final R rangeKey, final Function<Pair<H, R>, V> loadable) {
        final RangeTable<H, R, V> table = getTableWithReadLock();
        try {
            final V cachedValue = table.get(hashKey, rangeKey);
            if (cachedValue == null) {
                final V loadedValue = loadable.apply(Pair.of(hashKey, rangeKey));
                table.put(hashKey, rangeKey, loadedValue);
                return loadedValue;
            } else {
                return cachedValue;
            }
        } finally {
            tableLock.readLock().unlock();
        }
    }

    @Override
    public void delete(final H hashKey) {
        assertAllowedWriteWithoutBatch();
        final RangeTable<H, R, V> table = getTableWithReadLock();
        try {
            table.delete(hashKey);
        } finally {
            tableLock.readLock().unlock();
        }
    }

    @Override
    public void delete(final H hashKey, final R rangeKey) {
        assertAllowedWriteWithoutBatch();
        final RangeTable<H, R, V> table = getTableWithReadLock();
        try {
            table.delete(hashKey, rangeKey);
        } finally {
            tableLock.readLock().unlock();
        }
    }

    @Override
    public void deleteRange(final H hashKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock();
        try {
            table.deleteRange(hashKey);
        } finally {
            tableLock.readLock().unlock();
        }
    }

    @Override
    public void deleteRange(final H hashKey, final R fromRangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock();
        try {
            table.deleteRange(hashKey, fromRangeKey);
        } finally {
            tableLock.readLock().unlock();
        }
    }

    @Override
    public void deleteRange(final H hashKey, final R fromRangeKey, final R toRangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock();
        try {
            table.deleteRange(hashKey, fromRangeKey, toRangeKey);
        } finally {
            tableLock.readLock().unlock();
        }
    }

    @Override
    public void close() {
        tableLock.writeLock().lock();
        try {
            if (table != null) {
                table.close();
                table = null;
            }
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    public boolean isClosed() {
        tableLock.readLock().lock();
        try {
            return table == null;
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    @Override
    public void put(final H hashKey, final R rangeKey, final V value) {
        assertAllowedWriteWithoutBatch();
        final RangeTable<H, R, V> table = getTableWithReadLock();
        try {
            table.put(hashKey, rangeKey, value);
        } finally {
            tableLock.readLock().unlock();
        }
    }

    private void assertAllowedWriteWithoutBatch() {
        if (!allowPutWithoutBatch()) {
            throw new UnsupportedOperationException(
                    "Use newRangeBatch() for improved performance or override allowWriteWithoutBatch() if needed.");
        }
    }

    @Override
    public V get(final H hashKey, final R rangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock();
        try {
            return table.get(hashKey, rangeKey);
        } finally {
            tableLock.readLock().unlock();
        }
    }

    @Override
    public DelegateTableIterator<H, R, V> range(final H hashKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock();
        return new DelegateTableIterator<H, R, V>(table.range(hashKey), tableLock, allowHasNext());
    }

    public ICloseableIterator<V> rangeValues(final H hashKey) {
        return new DelegateValueTableIterator<V>(range(hashKey));
    }

    @Override
    public DelegateTableIterator<H, R, V> range(final H hashKey, final R fromRangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock();
        return new DelegateTableIterator<H, R, V>(table.range(hashKey, fromRangeKey), tableLock, allowHasNext());
    }

    public ICloseableIterator<V> rangeValues(final H hashKey, final R fromRangeKey) {
        return new DelegateValueTableIterator<V>(range(hashKey, fromRangeKey));
    }

    @Override
    public DelegateTableIterator<H, R, V> range(final H hashKey, final R fromRangeKey, final R toRangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock();
        return new DelegateTableIterator<H, R, V>(table.range(hashKey, fromRangeKey, toRangeKey), tableLock,
                allowHasNext());
    }

    public ICloseableIterator<V> rangeValues(final H hashKey, final R fromRangeKey, final R toRangeKey) {
        return new DelegateValueTableIterator<V>(range(hashKey, fromRangeKey, toRangeKey));
    }

    @Override
    public DelegateTableIterator<H, R, V> rangeReverse(final H hashKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock();
        return new DelegateTableIterator<H, R, V>(table.rangeReverse(hashKey), tableLock, allowHasNext());
    }

    public ICloseableIterator<V> rangeReverseValues(final H hashKey) {
        return new DelegateValueTableIterator<V>(rangeReverse(hashKey));
    }

    @Override
    public DelegateTableIterator<H, R, V> rangeReverse(final H hashKey, final R fromRangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock();
        return new DelegateTableIterator<H, R, V>(table.rangeReverse(hashKey, fromRangeKey), tableLock, allowHasNext());
    }

    public ICloseableIterator<V> rangeReverseValues(final H hashKey, final R fromRangeKey) {
        return new DelegateValueTableIterator<V>(rangeReverse(hashKey, fromRangeKey));
    }

    @Override
    public DelegateTableIterator<H, R, V> rangeReverse(final H hashKey, final R fromRangeKey, final R toRangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock();
        return new DelegateTableIterator<H, R, V>(table.rangeReverse(hashKey, fromRangeKey, toRangeKey), tableLock,
                allowHasNext());
    }

    public ICloseableIterator<V> rangeReverseValues(final H hashKey, final R fromRangeKey, final R toRangeKey) {
        return new DelegateValueTableIterator<V>(rangeReverse(hashKey, fromRangeKey, toRangeKey));
    }

    @Override
    public Batch<H, V> newBatch() {
        return newRangeBatch();
    }

    @Override
    public RangeBatch<H, R, V> newRangeBatch() {
        final RangeTable<H, R, V> table = getTableWithReadLock();
        return new DelegateRangeBatch<H, R, V>(table.newRangeBatch(), tableLock);
    }

    public Lock getTableReadLock() {
        return tableLock.readLock();
    }

    public static class DelegateRangeBatch<H_, R_, V_> implements RangeBatch<H_, R_, V_> {

        private final RangeBatch<H_, R_, V_> delegate;
        private final ReadWriteLock tableLockDelegate;
        private boolean closed = false;

        public DelegateRangeBatch(final RangeBatch<H_, R_, V_> delegate, final ReadWriteLock tableLockDelegate) {
            this.delegate = delegate;
            this.tableLockDelegate = tableLockDelegate;
        }

        @Override
        public void put(final H_ hashKey, final V_ value) {
            delegate.put(hashKey, value);
        }

        @Override
        public void delete(final H_ hashKey) {
            delegate.delete(hashKey);
        }

        @Override
        public void flush() {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                try {
                    delegate.flush();
                    delegate.close();
                    closed = true;
                } finally {
                    tableLockDelegate.readLock().unlock();
                }
            }
        }

        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            close();
        }

        @Override
        public void put(final H_ hashKey, final R_ rangeKey, final V_ value) {
            delegate.put(hashKey, rangeKey, value);
        }

        @Override
        public void delete(final H_ hashKey, final R_ rangeKey) {
            delegate.delete(hashKey, rangeKey);
        }

    }

    public static final class DelegateValueTableIterator<V_> implements ICloseableIterator<V_> {
        private final TableIterator<?, ?, V_> delegate;

        private DelegateValueTableIterator(final DelegateTableIterator<?, ?, V_> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public V_ next() {
            return delegate.next().getValue();
        }

        @Override
        public void remove() {
            delegate.remove();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    /**
     * internal iterator already handles auto close properly since ezdb 0.1.10
     */
    public static class DelegateTableIterator<_H, _R, _V> extends ACloseableIterator<TableRow<_H, _R, _V>>
            implements TableIterator<_H, _R, _V> {

        private final TableIterator<_H, _R, _V> delegate;
        private final ReadWriteLock tableLockDelegate;
        private final boolean allowHasNext;
        private boolean closed = false;

        public DelegateTableIterator(final TableIterator<_H, _R, _V> delegate, final ReadWriteLock tableLockDelegate,
                final boolean allowHasNext) {
            this.delegate = delegate;
            this.tableLockDelegate = tableLockDelegate;
            this.allowHasNext = allowHasNext;
        }

        @Override
        protected boolean innerHasNext() {
            if (allowHasNext) {
                final boolean hasNext = delegate.hasNext();
                return hasNext;
            } else {
                throw new UnsupportedOperationException(
                        "Do not use hasNext(), instead use next() and handle NoSuchElementException for better performance. If required, override allowHasNext() in table.");
            }
        }

        @Override
        protected TableRow<_H, _R, _V> innerNext() {
            return delegate.next();
        }

        @Override
        protected void innerRemove() {
            delegate.remove();
        }

        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            close();
        }

        @Override
        protected void innerClose() {
            if (!closed) {
                try {
                    delegate.close();
                    closed = true;
                } finally {
                    tableLockDelegate.readLock().unlock();
                }
            }
        }

    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        close();
    }

}
