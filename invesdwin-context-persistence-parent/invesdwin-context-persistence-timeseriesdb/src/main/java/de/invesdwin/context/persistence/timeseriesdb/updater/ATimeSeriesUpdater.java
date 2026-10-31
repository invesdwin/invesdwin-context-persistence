package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.log.Log;
import de.invesdwin.context.integration.retry.NonBlockingRetryLaterRuntimeException;
import de.invesdwin.context.integration.retry.RetryLaterPendingRuntimeException;
import de.invesdwin.context.persistence.ezdb.table.range.ADelegateRangeTable;
import de.invesdwin.context.persistence.timeseriesdb.ITimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.ITimeSeriesDBInternals;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdateProgress;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdaterInternalMethods;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ParallelUpdateProgress;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.SequentialChunkedUpdateProgress;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.SequentialContinuousUpdateProgress;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.iterable.FlatteningIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.skip.ASkippingIterable;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.graph.WaitForGraph;
import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLock;
import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLockRegistry;
import de.invesdwin.util.concurrent.lock.readwrite.IReentrantReadWriteLock;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.concurrent.reference.IMutableReference;
import de.invesdwin.util.concurrent.reference.MutableReference;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.streams.buffer.file.IMemoryMappedFile;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.date.millis.FDateMillis;

@NotThreadSafe
public abstract class ATimeSeriesUpdater<K, V> implements ITimeSeriesUpdater<K, V> {

    public static final int DEFAULT_BATCH_FLUSH_INTERVAL = ADelegateRangeTable.DEFAULT_BATCH_FLUSH_INTERVAL;
    public static final int BATCH_QUEUE_SIZE = 500_000 / DEFAULT_BATCH_FLUSH_INTERVAL;
    public static final boolean LARGE_COMPRESSOR = true;
    private static final Log LOG = new Log(ATimeSeriesUpdater.class);

    private final ISerde<V> valueSerde;
    private final ITimeSeriesDBInternals<K, V> table;
    private final TimeSeriesLookupStorageCache<K, V> lookupTable;
    private final File updateProgressFile;
    private final File updateFinishedFile;

    private final K key;
    @GuardedBy("none for performance")
    private String keyStr;
    private volatile String owner = HeartbeatFileChannelLockRegistry.HEARTBEAT_OWNER;
    private volatile FDate updateStart;
    private volatile FDate minTime = null;
    private volatile FDate maxTime = null;
    private final AtomicLong unflushedValueCount = new AtomicLong();
    private final AtomicLong flushedValueCount = new AtomicLong();
    private final AtomicLong lastFlushIndex = new AtomicLong();
    private final AtomicLong lastWriteUpdateProgressMillis = new AtomicLong(FDates.MIN_DATE.millisValue());
    private final ILock writeUpdateProgressLock;

    public ATimeSeriesUpdater(final K key, final ITimeSeriesDBInternals<K, V> table) {
        if (key == null) {
            throw new NullPointerException("key should not be null");
        }
        this.key = key;
        this.valueSerde = table.getValueSerde();
        this.table = table;
        this.lookupTable = table.getLookupTableCache(key);
        this.updateProgressFile = lookupTable.getUpdateProgressFile();
        this.updateFinishedFile = lookupTable.getUpdateFinishedFile();
        this.writeUpdateProgressLock = ILockCollectionFactory.getInstance(true)
                .newLock(updateProgressFile.getAbsolutePath() + "_writeUpdateProgressLock");
    }

    @Override
    public K getKey() {
        return key;
    }

    public final String getKeyStr() {
        if (keyStr == null) {
            keyStr = newKeyStr();
        }
        return keyStr;
    }

    protected String newKeyStr() {
        return table.innerHashKeyToString(key);
    }

    @Override
    public String getOwner() {
        return owner;
    }

    @Override
    public FDate getMinTime() {
        return minTime;
    }

    @Override
    public FDate getMaxTime() {
        return maxTime;
    }

    @Override
    public long getValueCount() {
        return flushedValueCount.get() + unflushedValueCount.get();
    }

    @Override
    public final TimeSeriesUpdaterResult update() throws IncompleteUpdateRetryableException {
        final IReentrantReadWriteLock segmentTableLock = table.getTableLock(key);
        /*
         * Make sure to release read locks in current thread when trying to acquire write lock
         * (https://stackoverflow.com/a/464824/67492). Also readers should be unlocked before trying to lock the table
         * for write lock acquisition so that everyone interested in this update unlocks to give one thread the chance
         * to acquire the write lock.
         */
        final ILock segmentReadLock = segmentTableLock.readLock();
        final int readHoldCount = segmentTableLock.getReadHoldCount();
        for (int i = 0; i < readHoldCount; i++) {
            segmentReadLock.unlock();
        }
        try {
            final ILock segmentWriteLock = segmentTableLock.writeLock();
            if (!segmentWriteLock.tryLock(TimeSeriesProperties.ACQUIRE_WRITE_LOCK_TIMEOUT)) {
                throw segmentWriteLock.getLockTrace()
                        .handleLockException(segmentWriteLock.getName(),
                                new RetryLaterPendingRuntimeException(RetryLaterPendingRuntimeException.PENDING_TAG +
                                        "Write lock could not be acquired for table [" + table.getName() + "] and key ["
                                                + key + "]. Please ensure all iterators are closed!"));
            }
            final File updateLockFile = new File(updateProgressFile.getAbsolutePath() + ".lock");
            final String graphKey = WaitForGraph.newKey(table.getName(), newKeyStr());
            final HeartbeatFileChannelLock updateLock = new HeartbeatFileChannelLock(updateLockFile) {
                @Override
                protected boolean isThreadLockEnabled() {
                    return true;
                }
            };
            try {
                if (!updateLock.tryLock()) {
                    return trackRemoteUpdate(updateLock, graphKey);
                }
                //register the heartbeat pin in the wait-for graph at acquisition time (not at result construction): the chained-wait danger lives in the doUpdate() below, where nested source demands can otherwise close a cycle onto this very pin
                WaitForGraph.pinAcquired(graphKey);
                try {
                    Files.deleteQuietly(updateFinishedFile);
                    Files.deleteQuietly(updateProgressFile);
                    this.updateStart = FDate.now();
                    onUpdateStarted(updateStart);
                    writeUpdateProgress(updateProgressFile, true);
                    doUpdate();
                    onUpdateFinished();
                    writeUpdateProgress(updateProgressFile, true);
                    return new TimeSeriesUpdaterResult(maxTime, updateLock, updateProgressFile, updateFinishedFile,
                            graphKey);
                } catch (final Throwable t) {
                    WaitForGraph.pinReleased(graphKey, Thread.currentThread());
                    updateLock.close();
                    throw IncompleteUpdateRetryableException.propagateIncompleteUpdateException(t);
                }
            } finally {
                segmentWriteLock.unlock();
            }
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            for (int i = 0; i < readHoldCount; i++) {
                segmentReadLock.lock();
            }
        }
    }

    private TimeSeriesUpdaterResult trackRemoteUpdate(final HeartbeatFileChannelLock updateLock,
            final String graphKey) throws IncompleteUpdateRetryableException {
        final IMutableReference<TimeSeriesUpdaterProgress> prevProgress = new MutableReference<>(
                TimeSeriesUpdaterProgress.EMPTY);
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(
                HeartbeatFileChannelLockRegistry.HEARTBEAT_INTERVAL);
        //register the pin wait in the wait-for graph and refuse it the moment the chain would close back onto this thread's own lock/pin/task: deterministic cycle abort, no timeout guessing; the caller's @Retry re-attempts the whole update once the chain has unwound
        WaitForGraph.beginWait(graphKey);
        try {
            while (true) {
                if (WaitForGraph.isSelfServedKey(graphKey)) {
                    //the task serving this very key pins it and its nested work demands it again: waiting for that
                    //pin would deadlock onto ourselves - adopt the current published state instead
                    LOG.warn("Self-served key [%s] pinned by this task: nested demand adopts the in-progress state", key);
                    break;
                }
                if (WaitForGraph.wouldCloseCycle(graphKey)) {
                    if (WaitForGraph.isCycleOwnedByOwnDemandTree(graphKey)) {
                        //pin held inside our own demand tree: the pinned store version is the one the tree is consuming; adopt the current state instead of waiting (see ALazyDataUpdater.maybeUpdate for the full rationale)
                        LOG.warn("Deferring update of table [%s] and key [%s]: the blocking heartbeat pin is held inside our own demand tree - continuing on the consistent in-tree state: %s", table.getName(), key, WaitForGraph.describeCycle(graphKey));
                        break;
                    }
                    if (!WaitForGraph.shouldFallThroughAsCycleVictim(graphKey)) {
                        //foreign cycle but not the designated victim edge: the max-key participant yields (see
                        //WaitForGraph.shouldFallThroughAsCycleVictim), this patience is finite in practice because
                        //the victim's unwind releases the pins this wait needs
                        continue;
                    }
                    //NonBlockingRetryLater (NOT IncompleteUpdateRetryableException): this wait happens BEFORE this task
                    //acquired its pin, so it has mutated nothing - a delete/rebuild at this point would destroy the
                    //concurrent rebuild we are waiting for. fallthrough (scope inherited from the submitting demand
                    //retry) makes the task-level @Retry propagate this instead of spinning, failing the future,
                    //waking the joiner and letting its pins close for a clean convergence later
                    throw new NonBlockingRetryLaterRuntimeException(
                            ATimeSeriesUpdater.class.getSimpleName() + ".trackRemoteUpdate: waiting for the heartbeat"
                                    + " pin of table [" + table.getName() + "] and key [" + key
                                    + "] would close a wait-for cycle inside this JVM - aborting this update attempt"
                                    + " retryably: " + WaitForGraph.describeCycle(graphKey));
                }
                if (updateFinishedFile.exists()) {
                    readUpdateProgress(prevProgress, updateFinishedFile);
                    break;
                }
                readUpdateProgress(prevProgress, updateProgressFile);
                if (loopCheck.checkClockNoInterrupt()) {
                    if (updateLock.tryLock()) {
                        try {
                            if (updateFinishedFile.exists()) {
                                readUpdateProgress(prevProgress, updateFinishedFile);
                                break;
                            } else {
                                throw new RetryLaterPendingRuntimeException(RetryLaterPendingRuntimeException.PENDING_TAG +
                                        "Update of another process timed out for table [" + table.getName() + "] and key ["
                                                + key + "]: " + updateLock.getFile().getAbsolutePath());
                            }
                        } finally {
                            updateLock.close();
                        }
                    } else if (HeartbeatFileChannelLockRegistry.isLockClaimedByThisJvm(updateLock.getFile())) {
                        /*
                         * The beating update lock is claimed by a still-open HeartbeatFileChannelLock instance of this
                         * very JVM whose holder is not part of a cycle this thread can see (e.g. a long legitimate
                         * iteration of another run): the graph check above refuses only truly cyclic waits, so this
                         * patience is safe. If it never resolves anyway, look for the "Finalizing unclosed
                         * TimeSeriesUpdaterResult" WARN carrying the creation stack trace of the leaked instance.
                         */
                        throw new RetryLaterPendingRuntimeException(RetryLaterPendingRuntimeException.PENDING_TAG + "Update lock of table [" + table.getName() + "] and key ["
                                + key + "] is beaten by this very JVM (owner ["
                                + HeartbeatFileChannelLockRegistry.HEARTBEAT_OWNER
                                + "]) although no local update() is active for this key: an unclosed"
                                + " TimeSeriesUpdaterResult leaked its HeartbeatFileChannelLock at ["
                                + updateLock.getFile().getAbsolutePath()
                                + "] - a same-JVM steal can never succeed, see the leaked-result WARNs in the"
                                + " log for the creation stack trace");
                    }
                }
                ALoggingTimeSeriesUpdater.FLUSH_LOG_INTERVAL.sleepNoInterrupt();
            }
        } finally {
            WaitForGraph.endWait(graphKey);
        }
        lookupTable.clearCaches();
        final FDate updatedTo = lookupTable.getLastValueEndTime();
        if (updatedTo == null) {
            //adoption found an EMPTY store (the pinning context wiped it for a clean-state recompute): nothing to
            //adopt and this task mutated nothing - plain retryable WITHOUT any delete on our side (wiping here would
            //race the in-progress rebuild)
            throw new NonBlockingRetryLaterRuntimeException("Adoption found empty store for table [" + table.getName() + "] and key [" + key + "] - retrying after the pinning update lands");
        }
        onUpdateFinished();
        return new TimeSeriesUpdaterResult(updatedTo, null, null, null, graphKey);
    }

    private void doUpdate() throws IncompleteUpdateRetryableException {
        try (TimeSeriesUpdateTransaction<V> updateTransaction = lookupTable
                .newUpdateTransaction(shouldRedoLastFile())) {
            final FDate updateFrom = updateTransaction.getUpdateFrom();
            final List<V> lastValues = updateTransaction.getLastValues();
            final long initialPrecedingMemoryOffset = updateTransaction.getPrecedingMemoryOffset();
            final long initialMemoryOffset = updateTransaction.getMemoryOffset();
            final long initialPrecedingValueCount = updateTransaction.getPrecedingValueCount();

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
                public TimeSeriesLookupStorageCache<K, V> getLookupTable() {
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
                public void onFlush(final ITimeSeriesUpdateProgress relativeProgress, final long flushIndex) {
                    lastFlushIndex.set(flushIndex);
                    unflushedValueCount.set(0);
                    flushedValueCount.addAndGet(relativeProgress.getValueCount());
                    if (minTime == null) {
                        minTime = relativeProgress.getMinTime();
                    }
                    maxTime = relativeProgress.getMaxTime();
                    ATimeSeriesUpdater.this.onFlush(relativeProgress, flushIndex);
                    writeUpdateProgress(updateProgressFile, false);
                }

                @Override
                public void onElement(final ITimeSeriesUpdateProgress relativeProgress, final long relativeCount) {
                    unflushedValueCount.addAndGet(relativeCount);
                    ATimeSeriesUpdater.this.onElement(relativeProgress, relativeCount);
                    writeUpdateProgress(updateProgressFile, false);
                }

                @Override
                public boolean shouldRedoLastFile() {
                    return ATimeSeriesUpdater.this.shouldRedoLastFile();
                }

            };
            final FlatteningIterable<? extends V> flatteningSources = new FlatteningIterable<>(lastValues,
                    skippingSource);

            if (shouldWriteInParallel()) {
                ParallelUpdateProgress.doUpdate(updateTransaction, internalMethods, initialPrecedingMemoryOffset,
                        initialMemoryOffset, initialPrecedingValueCount, flatteningSources);
            } else {
                if (IMemoryMappedFile.isSegmentSizeExceeded(Long.MAX_VALUE)) {
                    SequentialChunkedUpdateProgress.doUpdate(updateTransaction, internalMethods,
                            initialPrecedingMemoryOffset, initialMemoryOffset, initialPrecedingValueCount,
                            flatteningSources);
                } else {
                    SequentialContinuousUpdateProgress.doUpdate(updateTransaction, internalMethods,
                            initialPrecedingMemoryOffset, initialMemoryOffset, initialPrecedingValueCount,
                            flatteningSources);
                }
            }
        }
    }

    private void writeUpdateProgress(final File updateProgressFile, final boolean forced) {
        if (forced) {
            writeUpdateProgressLock.lock();
            try {
                final long nowMillis = FDateMillis.nowMillis();
                TimeSeriesUpdaterProgress.writeUpdateProgress(updateProgressFile, updateStart, lastFlushIndex.get(),
                        flushedValueCount.get(), minTime, maxTime);
                lastWriteUpdateProgressMillis.set(nowMillis);
            } finally {
                writeUpdateProgressLock.unlock();
            }
        } else {
            final long nowMillis = FDateMillis.nowMillis();
            if (ALoggingTimeSeriesUpdater.FLUSH_LOG_INTERVAL
                    .isLessThanMillis(nowMillis - lastWriteUpdateProgressMillis.get())) {
                if (writeUpdateProgressLock.tryLock()) {
                    try {
                        TimeSeriesUpdaterProgress.writeUpdateProgress(updateProgressFile, updateStart,
                                lastFlushIndex.get(), flushedValueCount.get(), minTime, maxTime);
                        lastWriteUpdateProgressMillis.set(nowMillis);
                    } finally {
                        writeUpdateProgressLock.unlock();
                    }
                }
            }
        }
    }

    private void readUpdateProgress(final IMutableReference<TimeSeriesUpdaterProgress> prevProgress,
            final File updateProgressFile) {
        final TimeSeriesUpdaterProgress progress = TimeSeriesUpdaterProgress.readUpdateProgress(updateProgressFile);
        if (progress == null) {
            return;
        }
        final TimeSeriesUpdaterProgress prevProgressValue = prevProgress.get();
        this.owner = progress.getOwner();
        final boolean firstProgress = this.updateStart == null;
        this.updateStart = progress.getUpdateStart();
        if (firstProgress) {
            onUpdateStarted(progress.getUpdateStart());
        }
        this.minTime = progress.getMinTime();
        this.maxTime = progress.getMaxTime();
        if (prevProgressValue.getFlushIndex() != progress.getFlushIndex()) {
            this.lastFlushIndex.set(progress.getFlushIndex());
            this.unflushedValueCount.set(0);
            this.flushedValueCount.set(progress.getValueCount());
            onFlush(progress.asRelativeProgress(prevProgressValue), progress.getFlushIndex());
        } else {
            final long unflushedValues = progress.getValueCount() - flushedValueCount.get();
            final long unflushedValuesBefore = unflushedValueCount.getAndSet(unflushedValues);
            final long relativeCount = unflushedValues - unflushedValuesBefore;
            onElement(progress.asRelativeProgress(prevProgressValue), relativeCount);
        }
    }

    protected boolean shouldWriteInParallel() {
        //LZ4HC should be compressed in parallel
        return Executors.getCpuThreadPoolCount() > 1;
    }

    protected boolean shouldRedoLastFile() {
        //redo last file so that we can update an incomplete last bar
        return true;
    }

    protected abstract ICloseableIterable<? extends V> getSource(FDate updateFrom)
            throws IncompleteUpdateRetryableException;

    protected abstract void onUpdateStarted(FDate updateStart);

    protected abstract void onUpdateFinished();

    protected abstract FDate extractStartTime(V element);

    protected abstract FDate extractEndTime(V element);

    @Override
    public Percent getProgress() {
        return getProgress(getMinTime(), getMaxTime());
    }

    public File getUpdateProgressFile() {
        return updateProgressFile;
    }

    public File getUpdateFinishedFile() {
        return updateFinishedFile;
    }

    protected abstract void onFlush(ITimeSeriesUpdateProgress relativeProgress, long flushIndex);

    protected abstract void onElement(ITimeSeriesUpdateProgress relativeProgress, long pendingCount);

}
