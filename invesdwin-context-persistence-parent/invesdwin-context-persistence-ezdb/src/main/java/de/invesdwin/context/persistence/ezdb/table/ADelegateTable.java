package de.invesdwin.context.persistence.ezdb.table;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

import org.iq80.leveldb.compression.LZ4Accessor;
import org.iq80.leveldb.compression.SnappyAccessor;

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
import de.invesdwin.util.assertions.Assertions;
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
import de.invesdwin.util.marshallers.serde.TypeDelegateSerde;
import de.invesdwin.util.shutdown.ShutdownHookManager;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FTimeUnit;
import ezdb.comparator.ComparableComparator;
import ezdb.comparator.LexicographicalComparator;
import ezdb.table.Batch;
import ezdb.table.Table;
import ezdb.table.TableRow;
import ezdb.util.TableIterator;

@ThreadSafe
public abstract class ADelegateTable<H, V> implements IDelegateTable<H, V> {

    public static final int DEFAULT_BATCH_FLUSH_INTERVAL = 10_000;

    protected final RangeTableInternalMethods internalMethods;
    private final IRangeTableDb db;
    private final IReadWriteLock tableLock;

    private final String name;
    private final File timestampFile;
    private final TableFinalizer<H, V> tableFinalizer;
    /**
     * used against too often accessing the timestampFile
     */
    private volatile FDate tableCreationTime;

    private final AtomicBoolean initializing = new AtomicBoolean();

    static {
        /*
         * prevent leveldb from complaining about snappy being null by making sure it is loaded earler via static
         * initializer
         */
        Assertions.checkNotNull(SnappyAccessor.SNAPPY, "SNAPPY");
        Assertions.checkNotNull(LZ4Accessor.LZ4FAST, "LZ4FAST");
        Assertions.checkNotNull(LZ4Accessor.LZ4HC, "LZ4HC");
    }

    public ADelegateTable(final String name) {
        this.name = name;
        this.internalMethods = new RangeTableInternalMethods(newHashKeySerde(), null, newValueSerde(),
                newHashKeyComparatorDisk(), null, newHashKeyComparatorMemory(), null, getBaseDirectory(),
                getDirectory());
        this.timestampFile = new File(new File(internalMethods.getDirectory(), getName()), "createdTimestamp");

        this.tableLock = Locks
                .newReentrantReadWriteLock(ADelegateTable.class.getSimpleName() + "_" + getName() + "_tableLock");
        this.db = newDB();
        this.tableFinalizer = new TableFinalizer<>();
    }

    protected File getDirectory() {
        return new File(getBaseDirectory(), ADelegateTable.class.getSimpleName());
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

    protected Comparator<Object> newHashKeyComparatorMemory() {
        return ComparableComparator.get();
    }

    protected Comparator<Object> newRangeKeyComparatorMemory() {
        return ComparableComparator.get();
    }

    @Override
    public TableIterator<? extends TableRow<H, V>> range() {
        final Table<H, V> table = getTableWithReadLock(false);
        return new DelegateTableIterator<H, V>(name, "range()", table.range(), getReadLock(false), allowHasNext());
    }

    @SuppressWarnings("unchecked")
    protected ISerde<V> newValueSerde() {
        final Class<V> type = (Class<V>) Reflections.resolveTypeArguments(getClass(), ADelegateTable.class)[2];
        return new TypeDelegateSerde<V>(type);
    }

    @SuppressWarnings("unchecked")
    protected ISerde<H> newHashKeySerde() {
        final Class<H> type = (Class<H>) Reflections.resolveTypeArguments(getClass(), ADelegateTable.class)[0];
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

    private Table<H, V> getTableWithReadLock(final boolean forUpdate) {
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
                return new ARetryCallable<Table<H, V>>(
                        new RetryOriginator(ADelegateTable.class, "initializeTableInitLocked", getName())) {
                    @Override
                    protected Table<H, V> callRetry() throws Exception {
                        return initializeTableInitLocked(readLock);
                    }
                }.call();
            } finally {
                initializing.set(false);
            }
        }
    }

    private Table<H, V> initializeTableInitLocked(final ILock readLock) {
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
                tableFinalizer.table = db.getTable(name);
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
                    tableFinalizer.table = db.getTable(name);
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

    protected void onDeleteTableFinished() {}

    protected boolean shouldPurgeTable() {
        return false;
    }

    @Override
    public void put(final H hashKey, final V value) {
        final Table<H, V> table = getTableWithReadLock(true);
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
        final Table<H, V> table = getTableWithReadLock(false);
        try {
            return table.get(hashKey);
        } finally {
            getReadLock(false).unlock();
        }
    }

    private V getValue(final TableRow<H, V> row) {
        if (row == null) {
            return null;
        } else {
            return row.getValue();
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
    public void delete(final H hashKey) {
        final Table<H, V> table = getTableWithReadLock(true);
        try {
            table.delete(hashKey);
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
    public Batch<H, V> newBatch() {
        final Table<H, V> table = getTableWithReadLock(true);
        return new DelegateBatch<H, V>(table.newBatch(), getReadLock(true));
    }

    public IReadWriteLock getTableLock() {
        return tableLock;
    }

    public boolean isEmpty() {
        final Table<H, V> table = getTableWithReadLock(false);
        try {
            final TableIterator<? extends TableRow<H, V>> range = table.range();
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

    public static class DelegateBatch<H_, V_> implements Batch<H_, V_> {

        private final BatchFinalizer<H_, V_> finalizer;

        public DelegateBatch(final Batch<H_, V_> delegate, final ILock tableReadLockDelegate) {
            this.finalizer = new BatchFinalizer<>(delegate, tableReadLockDelegate);
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

    }

    private static final class BatchFinalizer<__H, __V> extends AFinalizer {
        private Batch<__H, __V> delegate;
        private ILock tableReadLockDelegate;

        private BatchFinalizer(final Batch<__H, __V> delegate, final ILock tableReadLockDelegate) {
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
        private final TableIterator<? extends TableRow<?, V_>> delegate;

        private DelegateValueTableIterator(final DelegateTableIterator<?, V_> delegate) {
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
    public static class DelegateTableIterator<_H, _V> extends ACloseableIterator<TableRow<_H, _V>>
            implements TableIterator<TableRow<_H, _V>> {

        private final boolean allowHasNext;
        private final TableIteratorFinalizer<_H, _V> finalizer;

        public DelegateTableIterator(final String name, final String method,
                final TableIterator<? extends TableRow<_H, _V>> delegate, final ILock tableReadLockDelegate,
                final boolean allowHasNext) {
            super(new TextDescription("%s[%s].%s: %s", ADelegateTable.class.getSimpleName(), name,
                    DelegateTableIterator.class.getSimpleName(), method));
            this.allowHasNext = allowHasNext;
            this.finalizer = new TableIteratorFinalizer<_H, _V>(delegate, tableReadLockDelegate);
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
        protected TableRow<_H, _V> innerNext() {
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

    private static final class TableIteratorFinalizer<__H, __V> extends AFinalizer {
        private final TableIterator<? extends TableRow<__H, __V>> delegate;
        private ILock tableReadLockDelegate;

        private TableIteratorFinalizer(final TableIterator<? extends TableRow<__H, __V>> delegate,
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

    private static final class TableFinalizer<_H, _V> extends AFinalizer {
        private volatile Table<_H, _V> table;

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
