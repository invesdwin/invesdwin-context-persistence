package de.invesdwin.context.persistence.ezdb.table.range;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.integration.retry.task.ARetryCallable;
import de.invesdwin.context.integration.retry.task.RetryOriginator;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.persistence.ezdb.RangeTableCloseManager;
import de.invesdwin.context.persistence.ezdb.RangeTablePersistenceMode;
import de.invesdwin.context.persistence.ezdb.db.IRangeTableDb;
import de.invesdwin.context.persistence.ezdb.db.WriteThroughRangeTableDb;
import de.invesdwin.context.persistence.ezdb.db.storage.LevelDBJavaRangeTableDb;
import de.invesdwin.context.persistence.ezdb.db.storage.RangeTableInternalMethods;
import de.invesdwin.context.persistence.ezdb.db.storage.map.TreeMapRangeTableDb;
import de.invesdwin.context.persistence.ezdb.table.ADelegateTable;
import de.invesdwin.util.bean.tuple.Pair;
import de.invesdwin.util.collections.iterable.ACloseableIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.Locks;
import de.invesdwin.util.concurrent.lock.readwrite.IReadWriteLock;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.SerdeComparator;
import de.invesdwin.util.marshallers.serde.TypeDelegateSerde;
import de.invesdwin.util.shutdown.ShutdownHookManager;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FTimeUnit;
import ezdb.comparator.ComparableComparator;
import ezdb.comparator.LexicographicalComparator;
import ezdb.table.Batch;
import ezdb.table.RangeTableRow;
import ezdb.table.range.RangeBatch;
import ezdb.table.range.RangeTable;
import ezdb.util.TableIterator;

@ThreadSafe
public abstract class ADelegateRangeTable<H, R, V> implements IDelegateRangeTable<H, R, V> {

    public static final int DEFAULT_BATCH_FLUSH_INTERVAL = ADelegateTable.DEFAULT_BATCH_FLUSH_INTERVAL;

    protected final RangeTableInternalMethods internalMethods;
    private final IRangeTableDb db;
    private final IReadWriteLock tableLock;

    private final String name;
    private final File timestampFile;
    private final TableFinalizer<H, R, V> tableFinalizer;
    /**
     * used against too often accessing the timestampFile
     */
    private volatile FDate tableCreationTime;

    private final AtomicBoolean initializing = new AtomicBoolean();

    public ADelegateRangeTable(final String name) {
        this.name = name;
        this.internalMethods = new RangeTableInternalMethods(newHashKeySerde(), newRangeKeySerde(), newValueSerde(),
                newHashKeyComparatorDisk(), newRangeKeyComparatorDisk(), newHashKeyComparatorMemory(),
                newRangeKeyComparatorMemory(), getBaseDirectory(), getDirectory());
        this.timestampFile = new File(new File(internalMethods.getDirectory(), getName()), "createdTimestamp");

        this.tableLock = Locks
                .newReentrantReadWriteLock(ADelegateRangeTable.class.getSimpleName() + "_" + getName() + "_tableLock");
        this.db = newDB();
        this.tableFinalizer = new TableFinalizer<>();
    }

    protected File getDirectory() {
        return new File(getBaseDirectory(), ADelegateRangeTable.class.getSimpleName());
    }

    protected File getBaseDirectory() {
        return ContextProperties.getHomeDataDirectory();
    }

    /**
     * Default is disabling hasNext for improved performance.
     */
    protected boolean allowHasNext() {
        return false;
    }

    protected Comparator<java.nio.ByteBuffer> newHashKeyComparatorDisk() {
        //order is not so important on the hashkey, so use bytes only
        return new LexicographicalComparator();
    }

    protected Comparator<java.nio.ByteBuffer> newRangeKeyComparatorDisk() {
        return new SerdeComparator<R>(newRangeKeySerde());
    }

    protected Comparator<Object> newHashKeyComparatorMemory() {
        return ComparableComparator.get();
    }

    protected Comparator<Object> newRangeKeyComparatorMemory() {
        return ComparableComparator.get();
    }

    @SuppressWarnings("unchecked")
    protected ISerde<V> newValueSerde() {
        final Class<V> type = (Class<V>) Reflections.resolveTypeArguments(getClass(), ADelegateRangeTable.class)[2];
        return new TypeDelegateSerde<V>(type);
    }

    @SuppressWarnings("unchecked")
    protected ISerde<R> newRangeKeySerde() {
        final Class<R> type = (Class<R>) Reflections.resolveTypeArguments(getClass(), ADelegateRangeTable.class)[1];
        return new TypeDelegateSerde<R>(type);
    }

    @SuppressWarnings("unchecked")
    protected ISerde<H> newHashKeySerde() {
        final Class<H> type = (Class<H>) Reflections.resolveTypeArguments(getClass(), ADelegateRangeTable.class)[0];
        return new TypeDelegateSerde<H>(type);
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
        return new TreeMapRangeTableDb(internalMethods);
    }

    protected IRangeTableDb newDiskDb() {
        return new LevelDBJavaRangeTableDb(internalMethods);
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
        if (!initializing.compareAndSet(false, true)) {
            while (initializing.get()) {
                FTimeUnit.MILLISECONDS.sleepNoInterrupt(1);
            }
            return getTableWithReadLock(forUpdate);
        } else {
            try {
                return new ARetryCallable<RangeTable<H, R, V>>(
                        new RetryOriginator(ADelegateRangeTable.class, "initializeTableInitLocked", getName())) {
                    @Override
                    protected RangeTable<H, R, V> callRetry() throws Exception {
                        return initializeTableInitLocked(readLock);
                    }
                }.call();
            } finally {
                initializing.set(false);
            }
        }
    }

    private RangeTable<H, R, V> initializeTableInitLocked(final ILock readLock) {
        initializeTableInitLockedRetry(readLock, true);
        return tableFinalizer.table;
    }

    private void initializeTableInitLockedRetry(final ILock readLock, final boolean retry) {
        //otherwise initialize it with write lock (though check again because of lock switch)
        initializeTable();

        //and return the now not null table with read lock
        readLock.lock();
        if (tableFinalizer.table == null) {
            readLock.unlock();

            if (retry) {
                //retry immediately at least once to avoid unnecessary exceptions on the outside
                initializeTableInitLockedRetry(readLock, false);
            } else {
                throw new RetryLaterRuntimeException(
                        "table might have been deleted in the mean time, thus retry initialization");
            }
        }
    }

    private void maybePurgeTable() {
        if (!initializing.get() && shouldPurgeTable()) {
            //only purge if currently not used, might happen due to recursive computeIfAbsent with different loading functions
            if (tableLock.writeLock().tryLock()) {
                try {
                    //condition could have changed since lock has been acquired
                    if (!initializing.get() && shouldPurgeTable()) {
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
            initializeTableLocked();
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    private void initializeTableLocked() {
        if (tableFinalizer.table == null) {
            if (getTableCreationTime() == null) {
                if (getPersistenceMode().isDisk()) {
                    Files.touchQuietly(timestampFile);
                }
                tableCreationTime = new FDate();
            }
            try {
                tableFinalizer.table = db.getRangeTable(name);
                if (tableFinalizer.table == null) {
                    throw new IllegalStateException("table should not be null");
                }
                tableFinalizer.register(this);
                RangeTableCloseManager.register(this);
            } catch (final Throwable e) {
                if (Strings.containsIgnoreCase(e.getMessage(), "LOCK")) {
                    //ezdb.DbException: org.fusesource.leveldbjni.internal.NativeDB$DBException: IO error: lock /home/subes/Dokumente/Entwicklung/invesdwin/invesdwin-trading/invesdwin-trading-parent/invesdwin-trading-modules/invesdwin-trading-backtest/.invesdwin/de.invesdwin.context.persistence.leveldb.ADelegateRangeTable/CachingFinancialdataService_getInstrument/LOCK: Die Ressource ist zur Zeit nicht verfügbar
                    //at ezdb.leveldb.EzLevelDbTable.<init>(EzLevelDbTable.java:50)
                    //at ezdb.leveldb.EzLevelDb.getTable(EzLevelDb.java:69)
                    //at de.invesdwin.context.persistence.leveldb.ADelegateRangeTable.getTableWithReadLock(ADelegateRangeTable.java:144)
                    throw new RetryLaterRuntimeException(e);
                } else if (Throwables.isCausedByInterrupt(e)) {
                    //Caused by - ezdb.DbException: java.nio.channels.ClosedByInterruptException
                    //at ezdb.leveldb.table.range.EzLevelDbRangeTable.<init>(EzLevelDbRangeTable.java:60)
                    //at ezdb.leveldb.EzLevelDbJava.getRangeTable(EzLevelDbJava.java:98)
                    //at de.invesdwin.context.persistence.ezdb.db.storage.LevelDBJavaRangeTableDb.getRangeTable(LevelDBJavaRangeTableDb.java:79) *
                    throw new RetryLaterRuntimeException(e);
                } else {
                    Err.process(new RuntimeException("Table data for [" + getDirectory() + "/" + getName()
                            + "] is inconsistent. Resetting data and trying again.", e));
                    innerDeleteTable();
                    tableFinalizer.table = db.getRangeTable(name);
                    if (tableFinalizer.table == null) {
                        throw new IllegalStateException("table should not be null");
                    }
                    tableFinalizer.register(this);
                }
            }
        }
    }

    @Override
    public void deleteTable() {
        tableLock.writeLock().lock();
        try {
            innerDeleteTable();
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    private void innerDeleteTable() {
        onDeleteTablePreparing();
        if (tableFinalizer.table != null) {
            RangeTableCloseManager.unregister(this);
            tableFinalizer.table.close();
            tableFinalizer.table = null;
        }
        db.deleteTable(name);
        if (getPersistenceMode().isDisk()) {
            Files.deleteQuietly(timestampFile);
            final File tableDirectory = new File(internalMethods.getDirectory(), getName());
            final String[] list = tableDirectory.list();
            if (list != null && list.length > 0) {
                Files.deleteNative(tableDirectory);
            }
        }
        tableCreationTime = null;
        onDeleteTableFinished();
    }

    protected void onDeleteTablePreparing() {}

    protected void onDeleteTableFinished() {}

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
    public RangeTableRow<H, R, V> getLatest(final H hashKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock(false);
        try {
            return table.getLatest(hashKey);
        } finally {
            getReadLock(false).unlock();
        }
    }

    @Override
    public RangeTableRow<H, R, V> getLatest(final H hashKey, final R rangeKey) {
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

    private V getValue(final RangeTableRow<H, R, V> row) {
        if (row == null) {
            return null;
        } else {
            return row.getValue();
        }
    }

    @Override
    public RangeTableRow<H, R, V> getNext(final H hashKey, final R rangeKey) {
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

    private R getRangeKey(final RangeTableRow<H, R, V> row) {
        if (row == null) {
            return null;
        } else {
            return row.getRangeKey();
        }
    }

    @Override
    public RangeTableRow<H, R, V> getPrev(final H hashKey, final R rangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock(false);
        try {
            return table.getPrev(hashKey, rangeKey);
        } finally {
            getReadLock(false).unlock();
        }
    }

    @Override
    public V getOrLoad(final H hashKey, final Function<? super H, ? extends V> loadable) {
        final V cachedValue = get(hashKey);
        if (cachedValue == null) {
            //don't hold read lock while loading value
            final V loadedValue = loadable.apply(hashKey);
            //write lock is only for the actual table variable, not the table values, thus read lock is fine here
            if (loadedValue != null) {
                put(hashKey, loadedValue);
            }
            return loadedValue;
        } else {
            return cachedValue;
        }
    }

    @Override
    public V getOrLoad(final H hashKey, final R rangeKey, final Function<Pair<H, R>, V> loadable) {
        final V cachedValue = get(hashKey, rangeKey);
        if (cachedValue == null) {
            //don't hold read lock while loading value
            final V loadedValue = loadable.apply(Pair.of(hashKey, rangeKey));
            //write lock is only for the actual table variable, not the table values, thus read lock is fine here
            if (loadedValue != null) {
                put(hashKey, rangeKey, loadedValue);
            }
            return loadedValue;
        } else {
            return cachedValue;
        }
    }

    @Override
    public V getOrLoad(final H hashKey, final Supplier<V> loadable) {
        final V cachedValue = get(hashKey);
        if (cachedValue == null) {
            //don't hold read lock while loading value
            final V loadedValue = loadable.get();
            //write lock is only for the actual table variable, not the table values, thus read lock is fine here
            if (loadedValue != null) {
                put(hashKey, loadedValue);
            }
            return loadedValue;
        } else {
            return cachedValue;
        }
    }

    @Override
    public V getOrLoad(final H hashKey, final R rangeKey, final Supplier<V> loadable) {
        final V cachedValue = get(hashKey, rangeKey);
        if (cachedValue == null) {
            //don't hold read lock while loading value
            final V loadedValue = loadable.get();
            //write lock is only for the actual table variable, not the table values, thus read lock is fine here
            if (loadedValue != null) {
                put(hashKey, rangeKey, loadedValue);
            }
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
        if (initializing.get()) {
            return;
        }
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
    public DelegateRangeTableIterator<H, R, V> range() {
        final RangeTable<H, R, V> table = getTableWithReadLock(false);
        return new DelegateRangeTableIterator<H, R, V>(name, null, "range()", table.range(), getReadLock(false),
                allowHasNext());
    }

    public ICloseableIterator<V> rangeValues() {
        return new DelegateValueTableIterator<V>(range());
    }

    @Override
    public DelegateRangeTableIterator<H, R, V> range(final H hashKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock(false);
        return new DelegateRangeTableIterator<H, R, V>(name, hashKey, "range(final H hashKey)", table.range(hashKey),
                getReadLock(false), allowHasNext());
    }

    public ICloseableIterator<V> rangeValues(final H hashKey) {
        return new DelegateValueTableIterator<V>(range(hashKey));
    }

    @Override
    public DelegateRangeTableIterator<H, R, V> range(final H hashKey, final R fromRangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock(false);
        return new DelegateRangeTableIterator<H, R, V>(name, hashKey, "range(final H hashKey, final R fromRangeKey)",
                table.range(hashKey, fromRangeKey), getReadLock(false), allowHasNext());
    }

    public ICloseableIterator<V> rangeValues(final H hashKey, final R fromRangeKey) {
        return new DelegateValueTableIterator<V>(range(hashKey, fromRangeKey));
    }

    @Override
    public DelegateRangeTableIterator<H, R, V> range(final H hashKey, final R fromRangeKey, final R toRangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock(false);
        return new DelegateRangeTableIterator<H, R, V>(name, hashKey,
                "range(final H hashKey, final R fromRangeKey, final R toRangeKey)",
                table.range(hashKey, fromRangeKey, toRangeKey), getReadLock(false), allowHasNext());
    }

    public ICloseableIterator<V> rangeValues(final H hashKey, final R fromRangeKey, final R toRangeKey) {
        return new DelegateValueTableIterator<V>(range(hashKey, fromRangeKey, toRangeKey));
    }

    @Override
    public DelegateRangeTableIterator<H, R, V> rangeReverse() {
        final RangeTable<H, R, V> table = getTableWithReadLock(false);
        return new DelegateRangeTableIterator<H, R, V>(name, null, "range()", table.rangeReverse(), getReadLock(false),
                allowHasNext());
    }

    public ICloseableIterator<V> rangeReverseValues() {
        return new DelegateValueTableIterator<V>(rangeReverse());
    }

    @Override
    public DelegateRangeTableIterator<H, R, V> rangeReverse(final H hashKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock(false);
        return new DelegateRangeTableIterator<H, R, V>(name, hashKey, "rangeReverse(final H hashKey)",
                table.rangeReverse(hashKey), getReadLock(false), allowHasNext());
    }

    public ICloseableIterator<V> rangeReverseValues(final H hashKey) {
        return new DelegateValueTableIterator<V>(rangeReverse(hashKey));
    }

    @Override
    public DelegateRangeTableIterator<H, R, V> rangeReverse(final H hashKey, final R fromRangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock(false);
        return new DelegateRangeTableIterator<H, R, V>(name, hashKey,
                "rangeReverse(final H hashKey, final R fromRangeKey)", table.rangeReverse(hashKey, fromRangeKey),
                getReadLock(false), allowHasNext());
    }

    public ICloseableIterator<V> rangeReverseValues(final H hashKey, final R fromRangeKey) {
        return new DelegateValueTableIterator<V>(rangeReverse(hashKey, fromRangeKey));
    }

    @Override
    public DelegateRangeTableIterator<H, R, V> rangeReverse(final H hashKey, final R fromRangeKey, final R toRangeKey) {
        final RangeTable<H, R, V> table = getTableWithReadLock(false);
        return new DelegateRangeTableIterator<H, R, V>(name, hashKey,
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

    public boolean isEmpty() {
        final RangeTable<H, R, V> table = getTableWithReadLock(false);
        try {
            final TableIterator<? extends RangeTableRow<H, R, V>> range = table.range(null);
            try {
                final boolean hasNext = range.hasNext();
                return !hasNext;
            } finally {
                range.close();
            }
        } finally {
            getReadLock(true).unlock();
        }
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
        private RangeBatch<__H, __R, __V> delegate;
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
                delegate = null;
            } catch (final IOException e) {
                throw new RuntimeException(e);
            } finally {
                tableReadLockDelegate.unlock();
                tableReadLockDelegate = null;
            }
        }

        @Override
        protected boolean isCleaned() {
            return delegate == null;
        }

        @Override
        public boolean isThreadLocal() {
            return true;
        }
    }

    public static final class DelegateValueTableIterator<V_> implements ICloseableIterator<V_> {
        private final TableIterator<? extends RangeTableRow<?, ?, V_>> delegate;

        private DelegateValueTableIterator(final DelegateRangeTableIterator<?, ?, V_> delegate) {
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
    public static class DelegateRangeTableIterator<_H, _R, _V> extends ACloseableIterator<RangeTableRow<_H, _R, _V>>
            implements TableIterator<RangeTableRow<_H, _R, _V>> {

        private final boolean allowHasNext;
        private final TableIteratorFinalizer<_H, _R, _V> finalizer;

        public DelegateRangeTableIterator(final String name, final _H hashKey, final String method,
                final TableIterator<? extends RangeTableRow<_H, _R, _V>> delegate, final ILock tableReadLockDelegate,
                final boolean allowHasNext) {
            super(new TextDescription("%s[%s].%s[%s]: %s", ADelegateRangeTable.class.getSimpleName(), name,
                    DelegateRangeTableIterator.class.getSimpleName(), hashKey, method));
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
        protected RangeTableRow<_H, _R, _V> innerNext() {
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
        private final TableIterator<? extends RangeTableRow<__H, __R, __V>> delegate;
        private ILock tableReadLockDelegate;

        private TableIteratorFinalizer(final TableIterator<? extends RangeTableRow<__H, __R, __V>> delegate,
                final ILock tableReadLockDelegate) {
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
