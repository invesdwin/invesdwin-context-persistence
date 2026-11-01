package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
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
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
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
    //the epoch id of this attempt's update transaction (null for the legacy layout); the progress marker carries it as
    //its optional 7th field so that a demanding pass can see which live epoch owns the in-progress update
    private volatile String epochId;
    //epochs consumed by this pass ('<key>#<epochId>' entries), mirrored from the innermost EpochScopes scope while the
    //attempt runs so that the final marker writes keep them after the scope has been closed
    private volatile List<String> consumedEpochEntries;

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
     * The epoch id minted for this attempt's update transaction (epoch design), or null when this attempt runs on the
     * legacy shared data layout.
     */
    public String getEpochId() {
        return epochId;
    }

    /**
     * The epochs consumed by this updater's last attempt as '&lt;key&gt;#&lt;epochId&gt;' entries (epoch design M3-cp,
     * consumed re-validation): the ledger mirrored from the attempt's {@link EpochScopes} scope into the progress
     * marker. Empty when nothing was consumed (a normal published-state demand or a legacy marker). Pair with
     * {@link TimeSeriesUpdateTransaction#validateConsumedEpochs(TimeSeriesLookupStorageCache, List)} to re-check at
     * commit time that every consumed epoch is still PUBLISHED or LIVE.
     */
    public List<String> getConsumedEpochEntries() {
        final List<String> scopeEntries = EpochScopes.currentConsumedEntries();
        final List<String> entries = scopeEntries != null ? scopeEntries : this.consumedEpochEntries;
        return entries != null ? new ArrayList<>(entries) : new ArrayList<>();
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
                    //a reused updater instance must not leak the previous attempt's epoch into this attempt's marker
                    //writes and reclaim hooks before doUpdate mints the new one
                    this.epochId = null;
                    this.consumedEpochEntries = null;
                    this.updateStart = FDate.now();
                    onUpdateStarted(updateStart);
                    writeUpdateProgress(updateProgressFile, true);
                    doUpdate();
                    onUpdateFinished();
                    writeUpdateProgress(updateProgressFile, true);
                    //epoch design M3-cp: piggyback sweep of this key's dead epoch garbage at attempt finish (no new
                    //global timer); this attempt's own epoch is referenced by the generation it just published and
                    //stays, everything provably unreferenced, not heartbeat-live and older than
                    //RETAIN_OBSOLETE_EPOCHS_THRESHOLD goes
                    TimeSeriesEpochReclaim.reclaimObsoleteEpochs(lookupTable);
                    return new TimeSeriesUpdaterResult(maxTime, updateLock, updateProgressFile, updateFinishedFile,
                            graphKey);
                } catch (final Throwable t) {
                    WaitForGraph.pinReleased(graphKey, Thread.currentThread());
                    updateLock.close();
                    //epoch design M3-cp (a): this attempt's transaction was discarded on close, so its epoch local
                    //data files and its staging directory are garbage no reader will ever resolve - reclaim them
                    //best effort right away (the published-generation reference check still guards the rare case of
                    //a commit landing inside a throwing close)
                    TimeSeriesEpochReclaim.reclaimAbortedAttemptEpoch(lookupTable, epochId);
                    //and the same piggyback sweep as on the success path: a failed attempt may be the first one since
                    //a crashed writer left garbage
                    TimeSeriesEpochReclaim.reclaimObsoleteEpochs(lookupTable);
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
        //true when this wait ended on a pin held inside our own demand tree by an ancestor thread (not self-served):
        //the epoch that attempt names is visible to us, so an empty published store can be replaced by its staged
        //snapshot
        boolean inTreeAdoption = false;
        //true when this very task/pin holder re-demands its own key: the pass must NOT go through adoption-or-refuse
        //(epoch design M3-cp), it proceeds with the current published state, whatever it is
        boolean selfServedDemand = false;
        //register the pin wait in the wait-for graph and refuse it the moment the chain would close back onto this thread's own lock/pin/task: deterministic cycle abort, no timeout guessing; the caller's @Retry re-attempts the whole update once the chain has unwound
        WaitForGraph.beginWait(graphKey);
        try {
            while (true) {
                if (WaitForGraph.isSelfServedKey(graphKey)) {
                    //the task serving this very key pins it and its nested work demands it again: waiting for that
                    //pin would deadlock onto ourselves, and adopting or refusing a state this very pass produced
                    //would bootstrap-deadlock the mutual re-entrant demands - the demand simply proceeds with the
                    //current published state (possibly empty: our own output lands when this pass commits)
                    LOG.warn("Self-served key [%s] pinned by this task: the nested demand proceeds on the current"
                            + " published state without adoption (epoch design M3-cp self-served skip)", key);
                    selfServedDemand = true;
                    break;
                }
                if (WaitForGraph.wouldCloseCycle(graphKey)) {
                    if (WaitForGraph.isCycleOwnedByOwnDemandTree(graphKey)) {
                        //pin held inside our own demand tree: the pinned store version is the one the tree is consuming; adopt the current state instead of waiting (see ALazyDataUpdater.maybeUpdate for the full rationale)
                        LOG.warn("Deferring update of table [%s] and key [%s]: the blocking heartbeat pin is held inside our own demand tree - continuing on the consistent in-tree state: %s", table.getName(), key, WaitForGraph.describeCycle(graphKey));
                        inTreeAdoption = true;
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
        return adoptRemoteState(updateLock, graphKey, inTreeAdoption, selfServedDemand);
    }

    /**
     * Completes a remote-tracking wait (epoch design M3-cp semantics):
     * <ul>
     * <li>the published store always wins (unchanged legacy adoption);</li>
     * <li>a self-served demand (this very task/pin holder re-demands its own key) proceeds with the current
     * published state whatever it is - only here a null updatedTo may be reported: the demanding nested read is a
     * scheduling decision, not a value decision, and this pass's own output lands at its commit; the callers
     * tolerate a null updatedTo (ALazyDataUpdater.doUpdate null-checks it before use, and the live precalc driver
     * guards it on the trading side);</li>
     * <li>an empty store with an in-tree ancestor-owned pin may still be replaced by that attempt's staged epoch
     * snapshot (M2 in-epoch visibility);</li>
     * <li>anything else keeps the loud 'Adoption found empty store' NonBlockingRetryLater unchanged.</li>
     * </ul>
     */
    private TimeSeriesUpdaterResult adoptRemoteState(final HeartbeatFileChannelLock updateLock, final String graphKey,
            final boolean inTreeAdoption, final boolean selfServedDemand) throws NonBlockingRetryLaterRuntimeException {
        lookupTable.clearCaches();
        FDate updatedTo = lookupTable.getLastValueEndTime();
        if (updatedTo == null && inTreeAdoption) {
            updatedTo = tryAdoptEpochSnapshot(updateLock);
        }
        if (updatedTo == null && !selfServedDemand) {
            //adoption found an EMPTY store (the pinning context wiped it for a clean-state recompute): nothing to
            //adopt and this task mutated nothing - plain retryable WITHOUT any delete on our side (wiping here would
            //race the in-progress rebuild)
            throw new NonBlockingRetryLaterRuntimeException("Adoption found empty store for table [" + table.getName() + "] and key [" + key + "] - retrying after the pinning update lands");
        }
        onUpdateFinished();
        return new TimeSeriesUpdaterResult(updatedTo, null, null, null, graphKey);
    }

    /**
     * Epoch-aware adoption (M2): when the adoption above found an empty published store, but the progress marker of
     * this key names a live epoch whose heartbeat owner is still beating, and that epoch is visible to this thread's
     * demand tree, then the demanding pass reads the epoch's staged index snapshot instead of throwing. The snapshot
     * becomes a thread-confined, read-only view on the lookup cache (see
     * {@link TimeSeriesLookupStorageCache#adoptEpochSnapshot(String, List)} for the honest limits of that view), and
     * the consumed '&lt;key&gt;#&lt;epochId&gt;' entry is recorded into the current {@link EpochScopes} scope so the
     * pass's progress marker carries the ledger for the later commit-time re-check (that re-check is a follow-up
     * milestone, not part of M2). Returns null to fall through to the existing loud NonBlockingRetryLater when
     * anything about the epoch is not verifiably live - legacy stores without epochs keep today's behaviour exactly.
     */
    private FDate tryAdoptEpochSnapshot(final HeartbeatFileChannelLock updateLock) {
        final TimeSeriesUpdaterProgress progress = TimeSeriesUpdaterProgress.readUpdateProgress(updateProgressFile);
        if (progress == null || progress.getEpochId() == null) {
            //no epoch named (legacy marker or no attempt started): unchanged behaviour
            return null;
        }
        final String remoteEpochId = progress.getEpochId();
        if (!EpochScopes.isEpochVisibleToCurrentThread(remoteEpochId)) {
            //the epoch is not owned by this thread's demand tree: never expose a foreign epoch
            return null;
        }
        if (!isEpochOwnerLive(progress, updateLock.getFile())) {
            return null;
        }
        final List<MemoryFileSummary> stagedSummaries = TimeSeriesUpdateTransaction.readEpochIndexSnapshot(
                lookupTable, remoteEpochId);
        if (stagedSummaries == null || stagedSummaries.isEmpty()) {
            //the epoch has not landed a flush yet: nothing adoptable, keep the loud refusal
            return null;
        }
        FDate updatedTo = null;
        for (final MemoryFileSummary summary : stagedSummaries) {
            final FDate firstValueEndTime = summary.getFirstValueEndTime();
            if (firstValueEndTime != null && (updatedTo == null || firstValueEndTime.isAfterNotNullSafe(updatedTo))) {
                updatedTo = firstValueEndTime;
            }
        }
        if (updatedTo == null) {
            return null;
        }
        EpochScopes.recordConsumed(newKeyStr(), remoteEpochId);
        lookupTable.adoptEpochSnapshot(remoteEpochId, stagedSummaries);
        LOG.info("Adoption of table [%s] and key [%s] found an empty published store but adopts the staged epoch snapshot of the live in-tree epoch [%s]: %s summaries up to [%s] (in-epoch visibility)", table.getName(), key, remoteEpochId, stagedSummaries.size(), updatedTo);
        return updatedTo;
    }

    /**
     * Lease liveness of the epoch's heartbeat owner, reusing the heartbeat files maintained by
     * {@link HeartbeatFileChannelLockRegistry}: the lock claim of the in-progress update carries the owner id, its
     * sibling '.heartbeat' file is touched by the owner's registry while the attempt runs; a crashed owner goes stale
     * within HEARTBEAT_TIMEOUT and then the epoch is not adoptable. The same check lives in
     * {@link TimeSeriesEpochReclaim#isEpochOwnerLive(String, File)} for the reclaim/janitor paths.
     */
    private boolean isEpochOwnerLive(final TimeSeriesUpdaterProgress progress, final File updateLockFile) {
        return TimeSeriesEpochReclaim.isEpochOwnerLive(progress.getOwner(), updateLockFile);
    }

    private void doUpdate() throws IncompleteUpdateRetryableException {
        try (TimeSeriesUpdateTransaction<V> updateTransaction = lookupTable
                .newUpdateTransaction(shouldRedoLastFile())) {
            final FDate updateFrom = updateTransaction.getUpdateFrom();
            final List<V> lastValues = updateTransaction.getLastValues();
            final long initialPrecedingMemoryOffset = updateTransaction.getPrecedingMemoryOffset();
            final long initialMemoryOffset = updateTransaction.getMemoryOffset();
            final long initialPrecedingValueCount = updateTransaction.getPrecedingValueCount();
            this.epochId = updateTransaction.getEpochLocalDataId();
            this.consumedEpochEntries = null;
            //an epoch-scoped pass (epoch design M2): open the scope so nested demands on this thread inherit the
            //visible epoch chain, and stamp the progress marker with the epoch id right away, before the first flush,
            //so a demanding pass can identify the live epoch at any time during the attempt
            final EpochScopes.EpochScope epochScope = epochId != null ? EpochScopes.openEpoch(epochId) : null;
            try {
                if (epochId != null) {
                    writeUpdateProgress(updateProgressFile, true);
                    //epoch design M3-cp: write the (empty) index snapshot once at attempt start, right after the
                    //epoch was stamped into the marker, so the staging directory and snapshot exist before the first
                    //flush and the adoption gates (visible epoch + live heartbeat + snapshot present) can already be
                    //verified mid-first-batch; per-flush calls keep overwriting it from onFlush below
                    updateTransaction.writeEpochIndexSnapshot();
                }

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
                        //epoch design M2: at each flush, atomically re-publish the staged index snapshot of this epoch
                        //so that adoption in the tree of this attempt can read what has landed so far
                        updateTransaction.writeEpochIndexSnapshot();
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
            } finally {
                if (epochScope != null) {
                    epochScope.close();
                }
            }
        }
    }

    private void writeUpdateProgress(final File updateProgressFile, final boolean forced) {
        //the consumed ledger lives in the EpochScopes scope of the running attempt while it is open; mirror it onto
        //the updater so the final marker writes after the attempt keep the entries
        final List<String> scopeConsumedEntries = EpochScopes.currentConsumedEntries();
        if (scopeConsumedEntries != null) {
            this.consumedEpochEntries = scopeConsumedEntries;
        }
        if (forced) {
            writeUpdateProgressLock.lock();
            try {
                final long nowMillis = FDateMillis.nowMillis();
                TimeSeriesUpdaterProgress.writeUpdateProgress(updateProgressFile, updateStart, lastFlushIndex.get(),
                        flushedValueCount.get(), minTime, maxTime, epochId, consumedEpochEntries);
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
                                lastFlushIndex.get(), flushedValueCount.get(), minTime, maxTime, epochId,
                                consumedEpochEntries);
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
