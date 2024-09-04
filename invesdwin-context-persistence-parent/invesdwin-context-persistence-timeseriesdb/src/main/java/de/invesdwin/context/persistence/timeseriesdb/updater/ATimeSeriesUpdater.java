package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.persistence.ezdb.table.range.ADelegateRangeTable;
import de.invesdwin.context.persistence.timeseriesdb.ITimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.ITimeSeriesDBInternals;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateAbortedException;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.PrepareForUpdateResult;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdaterInternalMethods;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.IUpdateProgress;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ParallelUpdateProgress;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.SequentialUpdateProgress;
import de.invesdwin.util.collections.iterable.FlatteningIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.skip.ASkippingIterable;
import de.invesdwin.util.concurrent.lock.FileChannelLock;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.Locks;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public abstract class ATimeSeriesUpdater<K, V> implements ITimeSeriesUpdater<K, V> {

    public static final int DEFAULT_BATCH_FLUSH_INTERVAL = ADelegateRangeTable.DEFAULT_BATCH_FLUSH_INTERVAL;
    public static final int BATCH_QUEUE_SIZE = 500_000 / DEFAULT_BATCH_FLUSH_INTERVAL;
    public static final boolean LARGE_COMPRESSOR = true;

    private final ISerde<V> valueSerde;
    private final ITimeSeriesDBInternals<K, V> table;
    private final TimeSeriesStorageCache<K, V> lookupTable;
    private final File updateLockFile;

    private final K key;
    private volatile FDate minTime = null;
    private volatile FDate maxTime = null;
    private volatile int count = 0;

    public ATimeSeriesUpdater(final K key, final ITimeSeriesDBInternals<K, V> table) {
        if (key == null) {
            throw new NullPointerException("key should not be null");
        }
        this.key = key;
        this.valueSerde = table.getValueSerde();
        this.table = table;
        this.lookupTable = table.getLookupTableCache(key);
        this.updateLockFile = lookupTable.getUpdateLockFile();
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public FDate getMinTime() {
        return minTime;
    }

    @Override
    public FDate getMaxTime() {
        return maxTime;
    }

    public int getCount() {
        return count;
    }

    @Override
    public final boolean update() throws IncompleteUpdateRetryableException {
        final ILock writeLock = table.getTableLock(key).writeLock();
        try {
            if (!writeLock.tryLock(TimeSeriesProperties.ACQUIRE_WRITE_LOCK_TIMEOUT)) {
                throw Locks.getLockTrace()
                        .handleLockException(writeLock.getName(),
                                new RetryLaterRuntimeException(
                                        "Write lock could not be acquired for table [" + table.getName() + "] and key ["
                                                + key + "]. Please ensure all iterators are closed!"));
            }
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
        final File updateLockSyncFile = new File(updateLockFile.getAbsolutePath() + ".sync");
        try (FileChannelLock updateLockSyncFileLock = new FileChannelLock(updateLockSyncFile) {
            @Override
            protected boolean isThreadLockEnabled() {
                return true;
            }
        }) {
            if (!updateLockSyncFileLock.tryLock()) {
                throw new IncompleteUpdateRetryableException("Incomplete update found for table [" + table.getName()
                        + "] and key [" + key + "], need to clean everything up to restore all from scratch.");
            }
            Files.touchQuietly(updateLockFile);
            try {
                final Instant updateStart = new Instant();
                onUpdateStart();
                doUpdate();
                onUpdateFinished(updateStart);
                return true;
            } catch (final Throwable t) {
                throw propagateIncompleteUpdateException(t);
            } finally {
                Files.deleteQuietly(updateLockFile);
            }
        } finally {
            writeLock.unlock();
        }
    }

    protected IncompleteUpdateRetryableException propagateIncompleteUpdateException(final Throwable t)
            throws IncompleteUpdateRetryableException {
        if (Throwables.isCausedByType(t, IncompleteUpdateAbortedException.class)) {
            throw Throwables.propagate(t);
        }
        final IncompleteUpdateRetryableException incompleteException = Throwables.getCauseByType(t,
                IncompleteUpdateRetryableException.class);
        if (incompleteException != null) {
            return incompleteException;
        } else {
            return new IncompleteUpdateRetryableException("Something unexpected went wrong that could be retried", t);
        }
    }

    private void doUpdate() {
        final PrepareForUpdateResult<V> prepareForUpdateResult = lookupTable.prepareForUpdate(shouldRedoLastFile());
        final FDate updateFrom = prepareForUpdateResult.getUpdateFrom();
        final List<V> lastValues = prepareForUpdateResult.getLastValues();
        final long initialPrecedingMemoryOffset = prepareForUpdateResult.getPrecedingMemorOffset();
        final long initialMemoryOffset = prepareForUpdateResult.getMemoryOffset();
        final long initialPrecedingValueCount = prepareForUpdateResult.getPrecedingValueCount();

        final ICloseableIterable<? extends V> source = getSource(updateFrom);
        if (source == null) {
            throw new NullPointerException("source is null");
        }
        final ICloseableIterable<? extends V> skippingSource;
        if (updateFrom != null) {
            skippingSource = new ASkippingIterable<V>(source) {
                @Override
                protected boolean skip(final V element) {
                    final FDate endTime = extractEndTime(element);
                    //ensure we add no duplicate values
                    return endTime.isBeforeNotNullSafe(updateFrom);
                }
            };
        } else {
            skippingSource = source;
        }

        final ITimeSeriesUpdaterInternalMethods<K, V> internalMethods = new ITimeSeriesUpdaterInternalMethods<K, V>() {

            @Override
            public K getKey() {
                return key;
            }

            @Override
            public ISerde<V> getValueSerde() {
                return valueSerde;
            }

            @Override
            public TimeSeriesStorageCache<K, V> getLookupTable() {
                return lookupTable;
            }

            @Override
            public ITimeSeriesDB<K, V> getTable() {
                return table;
            }

            @Override
            public FDate extractStartTime(final V element) {
                return ATimeSeriesUpdater.this.extractStartTime(element);
            }

            @Override
            public FDate extractEndTime(final V element) {
                return ATimeSeriesUpdater.this.extractEndTime(element);
            }

            @Override
            public void onFlush(final int flushIndex, final IUpdateProgress<K, V> updateProgress) {
                count += updateProgress.getValueCount();
                if (minTime == null) {
                    minTime = updateProgress.getMinTime();
                }
                maxTime = updateProgress.getMaxTime();
                ATimeSeriesUpdater.this.onFlush(flushIndex, updateProgress);
            }

            @Override
            public void onElement(final IUpdateProgress<K, V> updateProgress) {
                ATimeSeriesUpdater.this.onElement(updateProgress);
            }

        };
        final FlatteningIterable<? extends V> flatteningSources = new FlatteningIterable<>(lastValues, skippingSource);

        if (shouldWriteInParallel()) {
            ParallelUpdateProgress.doUpdate(internalMethods, initialPrecedingMemoryOffset, initialMemoryOffset,
                    initialPrecedingValueCount, flatteningSources);
        } else {
            SequentialUpdateProgress.doUpdate(internalMethods, initialPrecedingMemoryOffset, initialMemoryOffset,
                    initialPrecedingValueCount, flatteningSources);
        }
    }

    protected boolean shouldWriteInParallel() {
        //LZ4HC should be compressed in parallel
        //TODO: there must still be a race condition with parallel writes that stops after the first chunk of 10k values
        return true;
    }

    protected boolean shouldRedoLastFile() {
        //redo last file so that we can update an incomplete last bar
        return true;
    }

    protected abstract ICloseableIterable<? extends V> getSource(FDate updateFrom);

    protected abstract void onUpdateFinished(Instant updateStart);

    protected abstract void onUpdateStart();

    protected abstract FDate extractStartTime(V element);

    protected abstract FDate extractEndTime(V element);

    @Override
    public Percent getProgress() {
        return getProgress(getMinTime(), getMaxTime());
    }

    protected abstract void onFlush(int flushIndex, IUpdateProgress<K, V> updateProgress);

    protected abstract void onElement(IUpdateProgress<K, V> updateProgress);

}
