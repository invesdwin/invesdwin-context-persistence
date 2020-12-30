package de.invesdwin.context.persistence.timeseries.ezdb;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.function.Function;

import javax.annotation.concurrent.ThreadSafe;

import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.persistence.timeseries.ezdb.db.IRangeTableDb;
import de.invesdwin.context.persistence.timeseries.ezdb.db.WriteThroughRangeTableDb;
import de.invesdwin.context.persistence.timeseries.serde.ExtendedTypeDelegateSerde;
import de.invesdwin.util.bean.tuple.Pair;
import de.invesdwin.util.collections.iterable.ACloseableIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.concurrent.future.Callables;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.Locks;
import de.invesdwin.util.concurrent.lock.readwrite.IReadWriteLock;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Strings;
import de.invesdwin.util.lang.description.TextDescription;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.shutdown.ShutdownHookManager;
import de.invesdwin.util.time.fdate.FDate;
import ezdb.RangeTable;
import ezdb.RawTableRow;
import ezdb.TableIterator;
import ezdb.TableRow;
import ezdb.batch.Batch;
import ezdb.batch.RangeBatch;
import ezdb.comparator.ComparableComparator;
import ezdb.comparator.LexicographicalComparator;
import ezdb.comparator.SerdeComparator;
import ezdb.leveldb.EzLevelDb;
import ezdb.leveldb.EzLevelDbJavaFactory;
import ezdb.serde.Serde;
import ezdb.treemap.object.EzObjectTreeMapDb;

@ThreadSafe
public abstract class ADelegateRangeTable<H, R, V> implements RangeTable<H, R, V> {

    private final Serde<H> hashKeySerde;
    private final Serde<R> rangeKeySerde;
    private final Serde<V> valueSerde;
    private final Comparator<byte[]> hashKeyComparatorDisk;
    private final Comparator<byte[]> rangeKeyComparatorDisk;
    private final Comparator<Object> hashKeyComparatorMemory;
    private final Comparator<Object> rangeKeyComparatorMemory;

    private final IRangeTableDb db;
    private final IReadWriteLock tableLock;

    private final String name;
    private final File directory;
    private final File timestampFile;
    private final TableFinalizer<H, R, V> tableFinalizer;
    /**
     * used against too often accessing the timestampFile
     */
    private volatile FDate tableCreationTime;

    public ADelegateRangeTable(final String name) {
        this.name = name;
        this.directory = getDirectory();
        this.timestampFile = new File(new File(directory, getName()), "createdTimestamp");

        this.hashKeySerde = newHashKeySerde();
        this.rangeKeySerde = newRangeKeySerde();
        this.valueSerde = newValueSerde();
        this.hashKeyComparatorDisk = newHashKeyComparatorDisk();
        this.rangeKeyComparatorDisk = newRangeKeyComparatorDisk();
        this.hashKeyComparatorMemory = newHashKeyComparatorMemory();
        this.rangeKeyComparatorMemory = newRangeKeyComparatorMemory();
        this.tableLock = Locks
                .newReentrantReadWriteLock(ADelegateRangeTable.class.getSimpleName() + "_" + getName() + "_tableLock");
        this.db = newDB();
        this.tableFinalizer = new TableFinalizer<>();
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

    protected Comparator<byte[]> newHashKeyComparatorDisk() {
        //order is not so important on the hashkey, so use bytes only
        return new LexicographicalComparator();
    }

    protected Comparator<byte[]> newRangeKeyComparatorDisk() {
        return new SerdeComparator<R>(rangeKeySerde);
    }

    protected Comparator<Object> newHashKeyComparatorMemory() {
        return ComparableComparator.get();
    }

    protected Comparator<Object> newRangeKeyComparatorMemory() {
        return ComparableComparator.get();
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

    protected RangeTablePersistenceMode getPersistenceMode() {
        return RangeTablePersistenceMode.DISK_ONLY;
    }

    protected IRangeTableDb newDB() {
        switch (getPersistenceMode()) {
        case DISK_ONLY:
            return newDiskDb();
        case MEMORY_WRITE_THROUGH_DISK:
            return newMemoryWriteThrougDiskDb();
        case MEMORY_ONLY:
            return newMemoryDb();
        default:
            throw UnknownArgumentException.newInstance(RangeTablePersistenceMode.class, getPersistenceMode());
        }
    }

    protected IRangeTableDb newMemoryWriteThrougDiskDb() {
        return new WriteThroughRangeTableDb(newMemoryDb(), newDiskDb());
    }

    protected IRangeTableDb newMemoryDb() {
        return new IRangeTableDb() {

            private final EzObjectTreeMapDb db = new EzObjectTreeMapDb();

            @Override
            public <_H, _R, _V> RangeTable<_H, _R, _V> getTable(final String tableName) {
                return db.getTable(tableName, null, null, null, hashKeyComparatorMemory, rangeKeyComparatorMemory);
            }

            @Override
            public void deleteTable(final String tableName) {
                db.deleteTable(tableName);
            }
        };
    }

    protected CompressionType newCompressionType() {
        return CompressionType.SNAPPY;
    }

    protected IRangeTableDb newDiskDb() {
        return new IRangeTableDb() {
            private final EzLevelDb db = new EzLevelDb(directory, new EzLevelDbJavaFactory() {
                @Override
                public DB open(final File path, final org.iq80.leveldb.Options options) throws IOException {
                    options.paranoidChecks(false);
                    //make sure snappy is enabled
                    options.compressionType(newCompressionType());
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

            @SuppressWarnings("unchecked")
            @Override
            public <_H, _R, _V> RangeTable<_H, _R, _V> getTable(final String tableName) {
                initDirectory();
                final RangeTable<H, R, V> table = db.getTable(tableName, hashKeySerde, rangeKeySerde, valueSerde,
                        hashKeyComparatorDisk, rangeKeyComparatorDisk);
                return (RangeTable<_H, _R, _V>) table;
            }

            @Override
            public void deleteTable(final String tableName) {
                db.deleteTable(tableName);
            }
        };
    }

    private void initDirectory() {
        try {
            Files.forceMkdir(directory);
        } catch (final IOException e) {
            throw Err.process(e);
        }
    }

    private RangeTable<H, R, V> getTableWithReadLock(final boolean forUpdate) {
        maybePurgeTable();
        //directly return table with read lock if not null
        final ILock readLock = getReadLock(forUpdate);
        readLock.lock();
        if (tableFinalizer.table != null) {
            return tableFinalizer.table;
        }
        readLock.unlock();

        //otherwise initialize it with write lock (though check again because of lock switch)
        initializeTable();

        //and return the now not null table with read lock
        readLock.lock();
        if (tableFinalizer.table == null) {
            readLock.unlock();
            throw new IllegalStateException("table should not be null here");
        }
        return tableFinalizer.table;
    }

    private void maybePurgeTable() {
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
    }

    private void initializeTable() {
        if (ShutdownHookManager.isShuttingDown()) {
            throw new RuntimeException("Shutting down");
        }
        tableLock.writeLock().lock();
        try {
            if (tableFinalizer.table == null) {
                if (getTableCreationTime() == null) {
                    if (getPersistenceMode().isDisk()) {
                        try {
                            Files.touch(timestampFile);
                        } catch (final IOException e) {
                            throw Err.process(e);
                        }
                    }
                    tableCreationTime = new FDate();
                }
                try {
                    tableFinalizer.table = db.getTable(name);
                    tableFinalizer.register(this);
                    RangeTableCloseManager.register(this);
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
                        tableFinalizer.table = db.getTable(name);
                        tableFinalizer.register(this);
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
        if (tableFinalizer.table != null) {
            RangeTableCloseManager.unregister(this);
            tableFinalizer.table.close();
            tableFinalizer.table = null;
        }
        db.deleteTable(name);
        if (getPersistenceMode().isDisk()) {
            Files.deleteQuietly(timestampFile);
            final File tableDirectory = new File(directory, getName());
            final String[] list = tableDirectory.list();
            if (list == null || list.length == 0) {
                Files.deleteNative(tableDirectory);
            }
        }
        tableCreationTime = null;
        onDeleteTableFinished();
    }

    protected void onDeleteTableFinished() {
    }

    protected boolean shouldPurgeTable() {
        return false;
    }

    @Override
    public void put(final H hashKey, final V value) {
        final RangeTable<H, R, V> table = getTableWithReadLock(true);
        try {
            table.put(hashKey, value);
        } finally {
            getReadLock(true).unlock();
        }
    }

    protected ILock getReadLock(final boolean forUpdate) {
        return tableLock.readLock();
    }

    @Override
    public V get(final H hashKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock(false);
        try {
            return table.get(hashKey);
        } finally {
            getReadLock(false).unlock();
        }
    }

    @Override
    public TableRow<H, R, V> getLatest(final H hashKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock(false);
        try {
            return table.getLatest(hashKey);
        } finally {
            getReadLock(false).unlock();
        }
    }

    @Override
    public TableRow<H, R, V> getLatest(final H hashKey, final R rangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock(false);
        try {
            return table.getLatest(hashKey, rangeKey);
        } finally {
            getReadLock(false).unlock();
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
        final RangeTable<H, R, V> table = getTableWithReadLock(false);
        try {
            return table.getNext(hashKey, rangeKey);
        } finally {
            getReadLock(false).unlock();
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
        final RangeTable<H, R, V> table = getTableWithReadLock(false);
        try {
            return table.getPrev(hashKey, rangeKey);
        } finally {
            getReadLock(false).unlock();
        }
    }

    public V getOrLoad(final H hashKey, final Function<H, V> loadable) {
        final V cachedValue = get(hashKey);
        if (cachedValue == null) {
            //don't hold read lock while loading value
            final V loadedValue = loadable.apply(hashKey);
            //write lock is only for the actual table variable, not the table values, thus read lock is fine here
            put(hashKey, loadedValue);
            return loadedValue;
        } else {
            return cachedValue;
        }
    }

    public V getOrLoad(final H hashKey, final R rangeKey, final Function<Pair<H, R>, V> loadable) {
        final V cachedValue = get(hashKey, rangeKey);
        if (cachedValue == null) {
            //don't hold read lock while loading value
            final V loadedValue = loadable.apply(Pair.of(hashKey, rangeKey));
            //write lock is only for the actual table variable, not the table values, thus read lock is fine here
            put(hashKey, rangeKey, loadedValue);
            return loadedValue;
        } else {
            return cachedValue;
        }
    }

    public V getOrLoad(final H hashKey, final Callable<V> loadable) {
        final V cachedValue = get(hashKey);
        if (cachedValue == null) {
            //don't hold read lock while loading value
            final V loadedValue = Callables.call(loadable);
            //write lock is only for the actual table variable, not the table values, thus read lock is fine here
            put(hashKey, loadedValue);
            return loadedValue;
        } else {
            return cachedValue;
        }
    }

    public V getOrLoad(final H hashKey, final R rangeKey, final Callable<V> loadable) {
        final V cachedValue = get(hashKey, rangeKey);
        if (cachedValue == null) {
            //don't hold read lock while loading value
            final V loadedValue = Callables.call(loadable);
            //write lock is only for the actual table variable, not the table values, thus read lock is fine here
            put(hashKey, rangeKey, loadedValue);
            return loadedValue;
        } else {
            return cachedValue;
        }
    }

    @Override
    public void delete(final H hashKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock(true);
        try {
            table.delete(hashKey);
        } finally {
            getReadLock(true).unlock();
        }
    }

    @Override
    public void delete(final H hashKey, final R rangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock(true);
        try {
            table.delete(hashKey, rangeKey);
        } finally {
            getReadLock(true).unlock();
        }
    }

    @Override
    public void deleteRange(final H hashKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock(true);
        try {
            table.deleteRange(hashKey);
        } finally {
            getReadLock(true).unlock();
        }
    }

    @Override
    public void deleteRange(final H hashKey, final R fromRangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock(true);
        try {
            table.deleteRange(hashKey, fromRangeKey);
        } finally {
            getReadLock(true).unlock();
        }
    }

    @Override
    public void deleteRange(final H hashKey, final R fromRangeKey, final R toRangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock(true);
        try {
            table.deleteRange(hashKey, fromRangeKey, toRangeKey);
        } finally {
            getReadLock(true).unlock();
        }
    }

    @Override
    public void close() {
        tableLock.writeLock().lock();
        try {
            if (tableFinalizer.table != null) {
                RangeTableCloseManager.unregister(this);
                tableFinalizer.table.close();
                tableFinalizer.table = null;
            }
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    public boolean isClosed() {
        tableLock.readLock().lock();
        try {
            return tableFinalizer.table == null;
        } finally {
            tableLock.readLock().unlock();
        }
    }

    @Override
    public void put(final H hashKey, final R rangeKey, final V value) {
        final RangeTable<H, R, V> table = getTableWithReadLock(true);
        try {
            table.put(hashKey, rangeKey, value);
        } finally {
            getReadLock(true).unlock();
        }
    }

    @Override
    public V get(final H hashKey, final R rangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock(false);
        try {
            return table.get(hashKey, rangeKey);
        } finally {
            getReadLock(false).unlock();
        }
    }

    @Override
    public DelegateTableIterator<H, R, V> range(final H hashKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock(false);
        return new DelegateTableIterator<H, R, V>(name, hashKey, "range(final H hashKey)", table.range(hashKey),
                getReadLock(false), allowHasNext());
    }

    public ICloseableIterator<V> rangeValues(final H hashKey) {
        return new DelegateValueTableIterator<V>(range(hashKey));
    }

    @Override
    public DelegateTableIterator<H, R, V> range(final H hashKey, final R fromRangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock(false);
        return new DelegateTableIterator<H, R, V>(name, hashKey, "range(final H hashKey, final R fromRangeKey)",
                table.range(hashKey, fromRangeKey), getReadLock(false), allowHasNext());
    }

    public ICloseableIterator<V> rangeValues(final H hashKey, final R fromRangeKey) {
        return new DelegateValueTableIterator<V>(range(hashKey, fromRangeKey));
    }

    @Override
    public DelegateTableIterator<H, R, V> range(final H hashKey, final R fromRangeKey, final R toRangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock(false);
        return new DelegateTableIterator<H, R, V>(name, hashKey,
                "range(final H hashKey, final R fromRangeKey, final R toRangeKey)",
                table.range(hashKey, fromRangeKey, toRangeKey), getReadLock(false), allowHasNext());
    }

    public ICloseableIterator<V> rangeValues(final H hashKey, final R fromRangeKey, final R toRangeKey) {
        return new DelegateValueTableIterator<V>(range(hashKey, fromRangeKey, toRangeKey));
    }

    @Override
    public DelegateTableIterator<H, R, V> rangeReverse(final H hashKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock(false);
        return new DelegateTableIterator<H, R, V>(name, hashKey, "rangeReverse(final H hashKey)",
                table.rangeReverse(hashKey), getReadLock(false), allowHasNext());
    }

    public ICloseableIterator<V> rangeReverseValues(final H hashKey) {
        return new DelegateValueTableIterator<V>(rangeReverse(hashKey));
    }

    @Override
    public DelegateTableIterator<H, R, V> rangeReverse(final H hashKey, final R fromRangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock(false);
        return new DelegateTableIterator<H, R, V>(name, hashKey, "rangeReverse(final H hashKey, final R fromRangeKey)",
                table.rangeReverse(hashKey, fromRangeKey), getReadLock(false), allowHasNext());
    }

    public ICloseableIterator<V> rangeReverseValues(final H hashKey, final R fromRangeKey) {
        return new DelegateValueTableIterator<V>(rangeReverse(hashKey, fromRangeKey));
    }

    @Override
    public DelegateTableIterator<H, R, V> rangeReverse(final H hashKey, final R fromRangeKey, final R toRangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock(false);
        return new DelegateTableIterator<H, R, V>(name, hashKey,
                "rangeReverse(final H hashKey, final R fromRangeKey, final R toRangeKey)",
                table.rangeReverse(hashKey, fromRangeKey, toRangeKey), getReadLock(false), allowHasNext());
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
        final RangeTable<H, R, V> table = getTableWithReadLock(true);
        return new DelegateRangeBatch<H, R, V>(table.newRangeBatch(), getReadLock(true));
    }

    public IReadWriteLock getTableLock() {
        return tableLock;
    }

    public static class DelegateRangeBatch<H_, R_, V_> implements RangeBatch<H_, R_, V_> {

        private final RangeBatchFinalizer<H_, R_, V_> finalizer;

        public DelegateRangeBatch(final RangeBatch<H_, R_, V_> delegate, final ILock tableReadLockDelegate) {
            this.finalizer = new RangeBatchFinalizer<>(delegate, tableReadLockDelegate);
            this.finalizer.register(this);
        }

        @Override
        public void put(final H_ hashKey, final V_ value) {
            finalizer.delegate.put(hashKey, value);
        }

        @Override
        public void delete(final H_ hashKey) {
            finalizer.delegate.delete(hashKey);
        }

        @Override
        public void flush() {
            finalizer.delegate.flush();
        }

        @Override
        public void close() throws IOException {
            finalizer.close();
        }

        @Override
        public void put(final H_ hashKey, final R_ rangeKey, final V_ value) {
            finalizer.delegate.put(hashKey, rangeKey, value);
        }

        @Override
        public void delete(final H_ hashKey, final R_ rangeKey) {
            finalizer.delegate.delete(hashKey, rangeKey);
        }

    }

    private static final class RangeBatchFinalizer<__H, __R, __V> extends AFinalizer {
        private final RangeBatch<__H, __R, __V> delegate;
        private ILock tableReadLockDelegate;

        private RangeBatchFinalizer(final RangeBatch<__H, __R, __V> delegate, final ILock tableReadLockDelegate) {
            this.delegate = delegate;
            this.tableReadLockDelegate = tableReadLockDelegate;
        }

        @Override
        protected void clean() {
            try {
                delegate.flush();
                delegate.close();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            } finally {
                tableReadLockDelegate.unlock();
                tableReadLockDelegate = null;
            }
        }

        @Override
        protected boolean isCleaned() {
            return tableReadLockDelegate == null;
        }

        @Override
        public boolean isThreadLocal() {
            return true;
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

        private final boolean allowHasNext;
        private final TableIteratorFinalizer<_H, _R, _V> finalizer;

        public DelegateTableIterator(final String name, final _H hashKey, final String method,
                final TableIterator<_H, _R, _V> delegate, final ILock tableReadLockDelegate,
                final boolean allowHasNext) {
            super(new TextDescription("%s[%s].%s[%s]: %s", ADelegateRangeTable.class.getSimpleName(), name,
                    DelegateTableIterator.class.getSimpleName(), hashKey, method));
            this.allowHasNext = allowHasNext;
            this.finalizer = new TableIteratorFinalizer<_H, _R, _V>(delegate, tableReadLockDelegate);
            this.finalizer.register(this);
        }

        @Override
        protected boolean innerHasNext() {
            if (allowHasNext) {
                final boolean hasNext = finalizer.delegate.hasNext();
                return hasNext;
            } else {
                throw new UnsupportedOperationException(
                        "Do not use hasNext(), instead use next() and handle NoSuchElementException for better performance. If required, override allowHasNext() in table.");
            }
        }

        @Override
        protected TableRow<_H, _R, _V> innerNext() {
            return finalizer.delegate.next();
        }

        @Override
        protected void innerRemove() {
            finalizer.delegate.remove();
        }

        @Override
        protected void innerClose() {
            finalizer.close();
        }

    }

    private static final class TableIteratorFinalizer<__H, __R, __V> extends AFinalizer {
        private final TableIterator<__H, __R, __V> delegate;
        private ILock tableReadLockDelegate;

        private TableIteratorFinalizer(final TableIterator<__H, __R, __V> delegate, final ILock tableReadLockDelegate) {
            this.delegate = delegate;
            this.tableReadLockDelegate = tableReadLockDelegate;
        }

        @Override
        protected void clean() {
            try {
                delegate.close();
            } finally {
                tableReadLockDelegate.unlock();
                tableReadLockDelegate = null;
            }
        }

        @Override
        protected boolean isCleaned() {
            return tableReadLockDelegate == null;
        }

        @Override
        public boolean isThreadLocal() {
            return true;
        }
    }

    private static final class TableFinalizer<_H, _R, _V> extends AFinalizer {
        private volatile RangeTable<_H, _R, _V> table;

        @Override
        protected void clean() {
            table.close();
            table = null;
        }

        @Override
        protected boolean isCleaned() {
            return table == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }
    }

}
