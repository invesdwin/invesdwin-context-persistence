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
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateYieldException;
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
import de.invesdwin.util.concurrent.lock.file.NameKeyedLocks;
import de.invesdwin.util.concurrent.lock.readwrite.IReentrantReadWriteLock;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.concurrent.reference.IMutableReference;
import de.invesdwin.util.concurrent.reference.MutableReference;
import de.invesdwin.util.error.Throwables;
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
    //the id of this attempt's update transaction, used by the reclaim hooks (null only before doUpdate mints it)
    private volatile String epochId;
    private volatile long lastLocalGhostFinalizeNudgeMillis;
    //diagnostic/nudge bookkeeping for orphaned local heartbeat claims (see trackRemoteUpdate)

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

    /**
     * The id minted for this attempt's data files (the epoch local layout, the only layout of this store); null only
     * while no update transaction of an attempt exists yet.
     */
    public String getEpochId() {
        return epochId;
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
                                new RetryLaterPendingRuntimeException(RetryLaterPendingRuntimeException.PENDING_PREFIX +
                                        "Write lock could not be acquired for table [" + table.getName() + "] and key ["
                                                + key + "]. Please ensure all iterators are closed!"));
            }
            //publish-then-yield: the attempt owns the table write lock for its pass, but every boundary commit of its
            //update transaction releases it (bounded re-acquisition) so iterators parked on the key's read side get a
            //window instead of freezing this writer's frontier; a failed re-acquisition ends the attempt cleanly at
            //the published boundary, and the lease keeps the end path releasing the lock exactly once
            final TimeSeriesWriteLockYield writeLockYield = new TimeSeriesWriteLockYield(segmentWriteLock,
                    TimeSeriesProperties.UPDATE_YIELD_ACQUIRE_WRITE_LOCK_TIMEOUT);
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
                    //a reused updater instance must not leak the previous attempt's id into the reclaim hooks before
                    //doUpdate mints the new one
                    this.epochId = null;
                    this.updateStart = FDate.now();
                    onUpdateStarted(updateStart);
                    writeUpdateProgress(updateProgressFile, true);
                    doUpdate(writeLockYield);
                    onUpdateFinished();
                    writeUpdateProgress(updateProgressFile, true);
                    //piggyback sweep of this key's dead attempt garbage at attempt finish (no new global timer); this
                    //attempt's own files are referenced by the generation it just published and stay, everything
                    //provably unreferenced and older than RETAIN_OBSOLETE_EPOCHS_THRESHOLD goes
                    TimeSeriesEpochReclaim.reclaimObsoleteEpochs(lookupTable);
                    return new TimeSeriesUpdaterResult(maxTime, updateLock, updateProgressFile, updateFinishedFile,
                            graphKey);
                } catch (final Throwable t) {
                    WaitForGraph.pinReleased(graphKey, Thread.currentThread());
                    updateLock.close();
                    //this attempt's transaction was discarded on close, so its attempt local data files are garbage
                    //no reader will ever resolve - reclaim them best effort right away (the published-generation
                    //reference check still guards the rare case of a commit landing inside a throwing close)
                    TimeSeriesEpochReclaim.reclaimAbortedAttemptEpoch(lookupTable, epochId);
                    //and the same piggyback sweep as on the success path: a failed attempt may be the first one since
                    //a crashed writer left garbage
                    TimeSeriesEpochReclaim.reclaimObsoleteEpochs(lookupTable);
                    final IncompleteUpdateYieldException yielded = Throwables.getCauseByType(t,
                            IncompleteUpdateYieldException.class);
                    if (yielded != null) {
                        //clean end of a publish-then-yield, not a failure: the attempt published its boundary, gave
                        //the readers their window and could not re-acquire the write lock - the published prefix is
                        //the resume point, so this attempt lost nothing and rides the same retryable rails as an
                        //aborted (source-death) attempt
                        throw new IncompleteUpdateRetryableException(
                                "Update attempt of table [" + table.getName() + "] and key [" + key
                                        + "] ended cleanly at a published segment boundary: it yielded the table write"
                                        + " lock for the readers of the key and could not re-acquire it within ["
                                        + TimeSeriesProperties.UPDATE_YIELD_ACQUIRE_WRITE_LOCK_TIMEOUT
                                        + "] - the next attempt of this key resumes from the published prefix",
                                yielded);
                    }
                    throw IncompleteUpdateRetryableException.propagateIncompleteUpdateException(t);
                }
            } finally {
                //the lease may already have released the write lock at a boundary commit and failed to take it back
                //(clean attempt end), so release through the lease, never blindly
                writeLockYield.releaseIfHeld();
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
        //true when this very task/pin holder re-demands its own key: the pass must NOT go through adoption-or-refuse,
        //it proceeds with the current published state, whatever it is
        boolean selfServedDemand = false;
        //register the pin wait in the wait-for graph and refuse it the moment the chain would close back onto this thread's own lock/pin/task: deterministic cycle abort, no timeout guessing; the caller's @Retry re-attempts the whole update once the chain has unwound
        WaitForGraph.beginWait(graphKey);
        try {
            while (true) {
                if (HeartbeatFileChannelLockRegistry.isLockClaimedByThisJvm(updateLock.getFile())
                        && !HeartbeatFileChannelLockRegistry.hasLiveLocalInstance(updateLock.getFile())) {
                    //same-JVM claim without a live registered instance: an orphaned result is stuck in the
                    //finalizer queue (a quiet heap delays it indefinitely) - nudge it so the NEXT tryLock
                    //iteration can steal the ghost claim instead of refusing the demand as cyclic
                    final long nowMillis = FDateMillis.nowMillis();
                    if (nowMillis - lastLocalGhostFinalizeNudgeMillis > 60000) {
                        lastLocalGhostFinalizeNudgeMillis = nowMillis;
                        LOG.warn("Forcing finalization for orphaned local heartbeat claim of table [%s] key [%s] - an unclosed TimeSeriesUpdaterResult is awaiting the warning finalizer; lastTryLockFailure=[%s]", table.getName(), key, updateLock.getLastTryLockFailureReason());
                        System.gc();
                        System.runFinalization();
                    }
                }
                if (WaitForGraph.isOwnPinHeldKey(graphKey)) {
                    //the task serving this very key pins it and its nested work demands it again: waiting for that
                    //pin would deadlock onto ourselves, and adopting or refusing a state this very pass produced
                    //would bootstrap-deadlock the mutual re-entrant demands - the demand simply proceeds with the
                    //current published state (possibly empty: our own output lands when this pass commits)
                    LOG.warn("Self-served key [%s] pinned by this task: the nested demand proceeds on the current"
                            + " published state without adoption", key);
                    selfServedDemand = true;
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
                                    + " retryably: " + WaitForGraph.describeCycle(graphKey)
                                    + " localClaim=[" + HeartbeatFileChannelLockRegistry.isLockClaimedByThisJvm(updateLock.getFile())
                                    + ",marker=" + readLockMarker(updateLock.getFile())
                                    + ",registered=" + HeartbeatFileChannelLockRegistry.describeLocalClaim(updateLock.getFile())
                                    + ",threadLock=" + describeThreadLock(updateLock.getFile())
                                    + ",tryLockFail=[" + updateLock.getLastTryLockFailureReason() + "]] " + WaitForGraph.describeAll());
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
                                throw new RetryLaterPendingRuntimeException(RetryLaterPendingRuntimeException.PENDING_PREFIX +
                                        "Update of another process timed out for table [" + table.getName() + "] and key ["
                                                + key + "]: " + updateLock.getFile().getAbsolutePath());
                            }
                        } finally {
                            updateLock.close();
                        }
                    } else if (HeartbeatFileChannelLockRegistry.isLockClaimedByThisJvm(updateLock.getFile())
                            && !HeartbeatFileChannelLockRegistry.hasLiveLocalInstance(updateLock.getFile())) {
                        //same-JVM claim with no live registered instance: an orphaned result awaiting finalization.
                        //Nudge the phantom through once per key/minute (quiet heaps delay finalization for ages,
                        //keeping the abandoned heartbeat claim beating and the OS file lock held)
                        final long nowMillis = FDateMillis.nowMillis();
                        if (nowMillis - lastLocalGhostFinalizeNudgeMillis > 60000) {
                            lastLocalGhostFinalizeNudgeMillis = nowMillis;
                            LOG.warn("Forcing finalization for orphaned local heartbeat claim of table [%s] key [%s] - an unclosed TimeSeriesUpdaterResult is awaiting the warning finalizer", table.getName(), key);
                            System.gc();
                            System.runFinalization();
                        }
                    } else if (HeartbeatFileChannelLockRegistry.isLockClaimedByThisJvm(updateLock.getFile())) {
                        /*
                         * The beating update lock is claimed by a still-open HeartbeatFileChannelLock instance of this
                         * very JVM whose holder is not part of a cycle this thread can see (e.g. a long legitimate
                         * iteration of another run): the graph check above refuses only truly cyclic waits, so this
                         * patience is safe. If it never resolves anyway, look for the "Finalizing unclosed
                         * TimeSeriesUpdaterResult" WARN carrying the creation stack trace of the leaked instance.
                         */
                        throw new RetryLaterPendingRuntimeException(RetryLaterPendingRuntimeException.PENDING_PREFIX + "Update lock of table [" + table.getName() + "] and key ["
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
        return adoptRemoteState(graphKey, selfServedDemand);
    }

    /**
     * Completes a remote-tracking wait:
     * <ul>
     * <li>the published store always wins (unchanged legacy adoption);</li>
     * <li>a self-served demand (this very task/pin holder re-demands its own key) proceeds with the current
     * published state whatever it is - only here a null updatedTo may be reported: the demanding nested read is a
     * scheduling decision, not a value decision, and this pass's own output lands at its commit; the callers
     * tolerate a null updatedTo (ALazyDataUpdater.doUpdate null-checks it before use, and the live precalc driver
     * guards it on the trading side);</li>
     * <li>anything else keeps the loud 'Adoption found empty store' NonBlockingRetryLater unchanged.</li>
     * </ul>
     */
    private TimeSeriesUpdaterResult adoptRemoteState(final String graphKey, final boolean selfServedDemand)
            throws NonBlockingRetryLaterRuntimeException {
        lookupTable.clearCaches();
        final FDate updatedTo = lookupTable.getLastValueEndTime();
        if (updatedTo == null && !selfServedDemand) {
            //adoption found an EMPTY store (the pinning context wiped it for a clean-state recompute): nothing to
            //adopt and this task mutated nothing - plain retryable WITHOUT any delete on our side (wiping here would
            //race the in-progress rebuild)
            throw new NonBlockingRetryLaterRuntimeException("Adoption found empty store for table [" + table.getName() + "] and key [" + key + "] - retrying after the pinning update lands");
        }
        onUpdateFinished();
        return new TimeSeriesUpdaterResult(updatedTo, null, null, null, graphKey);
    }

    private void doUpdate(final TimeSeriesWriteLockYield writeLockYield) throws IncompleteUpdateRetryableException {
        try (TimeSeriesUpdateTransaction<V> updateTransaction = lookupTable
                .newUpdateTransaction(shouldRedoLastFile())) {
            //this attempt holds the key's table write lock, so its boundary commits publish and then yield it
            updateTransaction.setWriteLockYield(writeLockYield);
            //consumers mirroring this store's frontier (if any) hear about every published boundary
            final ITimeSeriesBoundaryCommitListener boundaryCommitListener = newBoundaryCommitListener();
            if (boundaryCommitListener != null) {
                updateTransaction.setBoundaryCommitListener(boundaryCommitListener);
            }
            final FDate updateFrom = updateTransaction.getUpdateFrom();
            final List<V> lastValues = updateTransaction.getLastValues();
            final long initialPrecedingMemoryOffset = updateTransaction.getPrecedingMemoryOffset();
            final long initialMemoryOffset = updateTransaction.getMemoryOffset();
            final long initialPrecedingValueCount = updateTransaction.getPrecedingValueCount();
            this.epochId = updateTransaction.getEpochLocalDataId();
            final ICloseableIterable<? extends V> source = getSource(updateFrom);
            if (source == null) {
                throw new NullPointerException("source is null");
            }
            final ICloseableIterable<? extends V> skippingSource;
            if (updateFrom != null) {
                final FDate resumeAfterEndTime = updateTransaction.getResumeAfterEndTime();
                skippingSource = new ASkippingIterable<V>(source) {
                    @Override
                    protected boolean skip(final V element) {
                        final FDate endTime = extractEndTime(element);
                        if (resumeAfterEndTime != null && !endTime.isAfterNotNullSafe(resumeAfterEndTime)) {
                            //this attempt continues a published boundary of an attempt that died mid-pass: the store
                            //already holds everything up to that value, a source that hands it out again must not
                            //have it written a second time
                            return true;
                        }
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

                @Override
                public String getEpochLocalDataId() {
                    return updateTransaction.getEpochLocalDataId();
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
            //all values consumed without exception: publish the staged index now (close() alone must not commit a
            //possibly partial attempt)
            updateTransaction.commit();
        }
    }

    private String describeThreadLock(final File lockFile) {
        try {
            final ILock nameLock = NameKeyedLocks
                    .get(lockFile.getAbsoluteFile().toPath().toAbsolutePath().toString());
            return String.valueOf(nameLock.getLockTrace());
        } catch (final Throwable t2) {
            return "<unavailable:" + t2.getMessage() + ">";
        }
    }

    private String readLockMarker(final File lockFile) {
        try {
            final File marker = new File(lockFile.getAbsolutePath()
                    + HeartbeatFileChannelLockRegistry.HEARTBEAT_EXTENSION);
            final File source = marker.exists() ? marker : lockFile;
            if (!source.exists()) {
                return "<none>";
            }
            final String content = Files.readFileToString(source);
            return content.length() > 200 ? content.substring(0, 200) : content;
        } catch (final Throwable t2) {
            return "<unreadable:" + t2.getMessage() + ">";
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

    /**
     * The boundary-commit notification attached to this attempt's update transaction (null = silent, the default):
     * every segment boundary the attempt publishes reports its new frontier to the listener, so consumers above
     * the store can advance mirrored frontier keys with each published segment instead of only after a complete
     * update. The hook is read once per attempt at transaction creation.
     */
    protected ITimeSeriesBoundaryCommitListener newBoundaryCommitListener() {
        return null;
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
