package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.persistence.ezdb.table.range.ADelegateRangeTable;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateFoundException;
import de.invesdwin.context.persistence.timeseriesdb.PrepareForUpdateResult;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdaterInternalMethods;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.IUpdateProgress;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ParallelUpdateProgress;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.skip.ASkippingIterable;
import de.invesdwin.util.concurrent.lock.FileChannelLock;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.Locks;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public abstract class ATimeSeriesUpdater<K, V> implements ITimeSeriesUpdater<K, V> {

    public static final int BATCH_FLUSH_INTERVAL = ADelegateRangeTable.BATCH_FLUSH_INTERVAL;
    public static final int BATCH_QUEUE_SIZE = 500_000 / BATCH_FLUSH_INTERVAL;
    public static final boolean LARGE_COMPRESSOR = true;

    private final ISerde<V> valueSerde;
    private final ATimeSeriesDB<K, V> table;
    private final TimeSeriesStorageCache<K, V> lookupTable;
    private final File updateLockFile;

    private final K key;
    private volatile FDate minTime = null;
    private volatile FDate maxTime = null;
    private volatile int count = 0;

    public ATimeSeriesUpdater(final K key, final ATimeSeriesDB<K, V> table) {
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
    public final boolean update() throws IncompleteUpdateFoundException {
        final ILock writeLock = table.getTableLock(key).writeLock();
        try {
            if (!writeLock.tryLock(1, TimeUnit.MINUTES)) {
                throw Locks.getLockTrace()
                        .handleLockException(writeLock.getName(),
                                new RetryLaterRuntimeException(
                                        "Write lock could not be acquired for table [" + table.getName() + "] and key ["
                                                + key + "]. Please ensure all iterators are closed!"));
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        final File updateLockSyncFile = new File(updateLockFile.getAbsolutePath() + ".sync");
        try (FileChannelLock updateLockSyncFileLock = new FileChannelLock(updateLockSyncFile)) {
            if (updateLockSyncFile.exists() || !updateLockSyncFileLock.tryLock() || updateLockFile.exists()) {
                throw new IncompleteUpdateFoundException("Incomplete update found for table [" + table.getName()
                        + "] and key [" + key + "], need to clean everything up to restore all from scratch.");
            }
            try {
                try {
                    Files.touch(updateLockFile);
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
                final Instant updateStart = new Instant();
                onUpdateStart();
                doUpdate();
                onUpdateFinished(updateStart);
                Assertions.assertThat(updateLockFile.delete()).isTrue();
                return true;
            } catch (final Throwable t) {
                final IncompleteUpdateFoundException incompleteException = Throwables.getCauseByType(t,
                        IncompleteUpdateFoundException.class);
                if (incompleteException != null) {
                    throw incompleteException;
                } else {
                    throw new IncompleteUpdateFoundException("Something unexpected went wrong", t);
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void doUpdate() {
        final PrepareForUpdateResult<V> prepareForUpdateResult = lookupTable.prepareForUpdate(shouldRedoLastFile());
        final FDate updateFrom = prepareForUpdateResult.getUpdateFrom();
        final List<V> lastValues = prepareForUpdateResult.getLastValues();
        final long initialAddressOffset = prepareForUpdateResult.getAddressOffset();

        final ICloseableIterable<? extends V> source = getSource(updateFrom);
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
            public ATimeSeriesDB<K, V> getTable() {
                return table;
            }

            @Override
            public FDate extractEndTime(final V element) {
                return ATimeSeriesUpdater.this.extractEndTime(element);
            }

            @Override
            public void onFlush(final int flushIndex, final IUpdateProgress<K, V> progress) {
                count += progress.getValueCount();
                if (minTime == null) {
                    minTime = progress.getMinTime();
                }
                maxTime = progress.getMaxTime();
                ATimeSeriesUpdater.this.onFlush(flushIndex, progress);
            }

        };

        ParallelUpdateProgress.doUpdate(internalMethods, lastValues, initialAddressOffset, skippingSource);
    }

    protected boolean shouldRedoLastFile() {
        return true;
    }

    protected abstract ICloseableIterable<? extends V> getSource(FDate updateFrom);

    protected abstract void onUpdateFinished(Instant updateStart);

    protected abstract void onUpdateStart();

    protected abstract FDate extractEndTime(V element);

    protected abstract void onFlush(int flushIndex, IUpdateProgress<K, V> updateProgress);

}
