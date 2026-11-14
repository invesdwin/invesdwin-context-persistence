package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import org.apache.commons.lang3.BooleanUtils;

import org.springframework.retry.backoff.BackOffPolicy;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.integration.concurrent.DatabaseThreads;
import de.invesdwin.context.integration.concurrent.nonblocking.ANonBlockingRunnable;
import de.invesdwin.context.integration.concurrent.nonblocking.DisabledNonBlockingRunnable;
import de.invesdwin.context.integration.concurrent.nonblocking.INonBlockingRunnable;

import de.invesdwin.context.integration.retry.NonBlockingRetryLaterRuntimeException;
import de.invesdwin.context.integration.retry.RetryScopes;
import de.invesdwin.context.integration.retry.fast.FastNonBlockingRetryLaterRuntimeException;
import de.invesdwin.context.integration.retry.task.ARetryCallable;
import de.invesdwin.context.integration.retry.task.BackOffPolicies;
import de.invesdwin.context.integration.retry.task.RetryOriginator;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.concurrent.graph.WaitForGraph;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.IReentrantLock;
import de.invesdwin.util.concurrent.lock.Locks;
import de.invesdwin.util.concurrent.lock.readwrite.IReentrantReadWriteLock;
import de.invesdwin.util.concurrent.nested.ANestedExecutor;
import de.invesdwin.util.concurrent.taskinfo.provider.TaskInfoCallable;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.duration.Duration;
import io.netty.util.concurrent.FastThreadLocal;

@ThreadSafe
public abstract class ALazyDataUpdater<K, V> implements ILazyDataUpdater<K, V> {

    public static final String REASON_DOES_NOT_EQUAL = "does not equal";
    public static final String REASON_IS_BEFORE = "is before";

    //the wait the update attempt loop takes between attempts; it is always applied with the table write lock released,
    //and by default it is the same exponential schedule the retry aspect of an update body used before
    private static final Duration UPDATE_ATTEMPT_BACK_OFF_MIN = Duration.ONE_MILLISECOND;
    private static final Duration UPDATE_ATTEMPT_BACK_OFF_MAX = Duration.ONE_MINUTE;

    private static final FastThreadLocal<Boolean> SKIP_UPDATE_ON_CURRENT_THREAD_IF_ALREADY_RUNNING = new FastThreadLocal<Boolean>();

    //shares the in-memory update lock between updater instances targeting the SAME physical store key (updaterId embeds the absolute shared version directory path): two contexts racing to update one store is what produced the heartbeat-pin-vs-join cross edges in the precalculated livelock (and it fragments the wait-for graph with multiple owners per key). Single-flight per key inside this JVM removes that class at its source; other JVMs stay serialised by the heartbeat file lock. Weak values: ids move with store versions over the lifetime of a process, so entries must vanish once no updater instance or parked thread references the lock anymore (same pattern as the file channel thread locks)
    private static final Cache<String, IReentrantLock> SHARED_UPDATE_LOCKS = Caffeine.newBuilder().weakValues().build();
    protected final Log log = new Log(this);
    private final K key;
    @GuardedBy("getUpdateLock()")
    private volatile FDate lastUpdateCheck = FDates.MIN_DATE;
    @GuardedBy("getUpdateLock()")
    private volatile int lastResetIndex = Integer.MIN_VALUE;
    @GuardedBy("this")
    private IReentrantLock updateLock;
    @GuardedBy("this")
    private Future<?> updateFuture;
    @GuardedBy("none for performance")
    private String updaterId;
    private ANonBlockingRunnable maybeUpdateNonBlocking;
    private ANonBlockingRunnable maybeUpdateNonBlockingForce;
    private String keyStr;

    public ALazyDataUpdater(final K key) {
        if (key == null) {
            throw new NullPointerException("key should not be null");
        }
        this.key = key;
    }

    public final String getUpdaterId() {
        if (updaterId == null) {
            updaterId = newUpdaterId();
        }
        return updaterId;
    }

    protected String newUpdaterId() {
        return ALazyDataUpdater.class.getSimpleName() + "_" + getTable().getName() + "_"
                + getTable().getLookupTableCache(getKey())
                        .getDirectoryHashKey()
                        .getDirectoryHashKeyVersion()
                        .getDirectoryHashKeyVersionShared()
                        .getAbsolutePath()
                + "_" + getKeyStr() + "_" + getElementsName();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(getKeyStr()).toString();
    }

    public final K getKey() {
        return key;
    }

    public final String getKeyStr() {
        if (keyStr == null) {
            keyStr = newKeyStr();
        }
        return keyStr;
    }

    protected String newKeyStr() {
        return getTable().innerHashKeyToString(key);
    }

    private synchronized IReentrantLock getUpdateLock() {
        if (updateLock == null) {
            updateLock = SHARED_UPDATE_LOCKS.get(getUpdaterId() + "_updateLock",
                    k -> Locks.newReentrantLock(k));
        }
        return updateLock;
    }

    protected abstract ANestedExecutor getNestedExecutor();

    public static void setSkipUpdateOnCurrentThreadIfAlreadyRunning(final boolean skipUpdateIfRunning) {
        if (skipUpdateIfRunning) {
            SKIP_UPDATE_ON_CURRENT_THREAD_IF_ALREADY_RUNNING.set(true);
        } else {
            SKIP_UPDATE_ON_CURRENT_THREAD_IF_ALREADY_RUNNING.remove();
        }
    }

    public static boolean isSkipUpdateOnCurrentThreadIfAlreadyRunning() {
        return BooleanUtils.isTrue(SKIP_UPDATE_ON_CURRENT_THREAD_IF_ALREADY_RUNNING.get());
    }

    @Override
    public INonBlockingRunnable maybeUpdateNonBlocking(final boolean force) {
        if (force) {
            if (maybeUpdateNonBlockingForce == null) {
                synchronized (this) {
                    if (maybeUpdateNonBlockingForce == null) {
                        maybeUpdateNonBlockingForce = new NonBlockingMaybeUpdateForce(getClass(), getKeyStr());
                    }
                }
            }
            maybeUpdateNonBlockingForce.resetIfDone();
            if (maybeUpdateNonBlocking != null) {
                maybeUpdateNonBlocking.resetIfDone();
            }
            return maybeUpdateNonBlockingForce;
        } else if (shouldCheckForUpdate()) {
            if (maybeUpdateNonBlocking == null) {
                synchronized (this) {
                    if (maybeUpdateNonBlocking == null) {
                        maybeUpdateNonBlocking = new NonBlockingMaybeUpdate(getClass(), getKeyStr());
                    }
                }
            }
            maybeUpdateNonBlocking.resetIfDone();
            return maybeUpdateNonBlocking;
        } else {
            return DisabledNonBlockingRunnable.INSTANCE;
        }
    }

    @Override
    public final boolean maybeUpdate(final boolean force) {
        final FDate newUpdateCheck = FDate.now();
        if (force || shouldCheckForUpdate(newUpdateCheck)) {
            final String graphKey = WaitForGraph.newKey(getTable().getName(), getKeyStr());
            if (skipSelfServedDemand(graphKey)) {
                return false;
            }
            //close-before-wait (blocker-5): a demand read must never wait for this key's update while it is itself
            //holding that key's read side. The demand of a range read is very often issued from inside that very read
            //(the gap loader of a historical query asking for more values while the query iterator is still walking the
            //table), and the update attempt refuses exactly then with the skip-if-readers verdict - while the iterators
            //holding the read side belong to the waiting readers themselves. Nothing can ever close, so the store never
            //advances and the demand retries forever (674 skips over 49 minutes in the cap-25 r7 matrix). Parking the
            //caller's own read holds for the duration of the wait - and taking them back afterwards, the same
            //release/re-lock pair ATimeSeriesUpdater.update() applies to its own write acquisition - is what lets the
            //writer this demand is waiting for land. Foreign holders are untouched: they still refuse the attempt.
            final int parkedReadHolds = parkOwnTableReadHolds();
            try {
                return maybeUpdateWaiting(newUpdateCheck, force, graphKey);
            } finally {
                restoreOwnTableReadHolds(parkedReadHolds);
            }
        }
        return false;
    }

    /**
     * The waiting half of a demand: update lock, task hand-over and the join - everything that must not run while the
     * demanding thread still occupies the read side of the table it is waiting for.
     */
    private boolean maybeUpdateWaiting(final FDate newUpdateCheck, final boolean force, final String graphKey) {
        //Deadlock avoidance: never add a wait edge that closes a wait-for cycle inside this JVM (the permanent precalculated livelock formed exactly like that). If the target update's progress transitively depends on a lock, pin, or task held by this very thread, abort this attempt retryably; later attempts run once the chain has unwound
        if (skipCycleForCaller(graphKey)) {
            return false;
        }
        final IReentrantLock updateLock = getUpdateLock();
        if (shouldSkipUpdateOnCurrentThreadIfAlreadyRunning()) {
            try {
                if (!updateLock.tryLock(TimeSeriesProperties.ACQUIRE_UPDATE_LOCK_TIMEOUT)) {
                    //do NOT let the caller proceed believing the update happened when another context holds the update lock past the timeout: that reads interior holes the pending update fills later, making results depend on thread scheduling - surface it as a retry instead (same policy as the table write-lock case below)
                    throw new FastNonBlockingRetryLaterRuntimeException(
                            ALazyDataUpdater.class.getSimpleName() + ".maybeUpdate: update of "
                                    + getElementsName() + " [" + getKeyStr()
                                    + "] blocked because the update lock is held by another context past the"
                                    + " acquire timeout - retrying instead of reading an incomplete table");
                }
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }
        } else {
            updateLock.lock();
        }
        WaitForGraph.lockAcquired(graphKey);
        try {
            if (!force && !shouldCheckForUpdate(newUpdateCheck)) {
                //some sort of double checked locking to skip if someone else came before us
                return false;
            }
            Future<?> updateFutureCopy = updateFuture;
            final String reason;
            final Thread submittingThread = Thread.currentThread();
            //explicit scope inheritance: if this demand call runs inside an outer retry, the task body must
            //see it (fallThroughToOuterRetry sites) so refusals propagate instead of retrying under the write lock
            final int outerRetryDepth = RetryScopes.capture();
            if (updateFutureCopy == null
                    || updateFutureCopy.isDone() && (force || shouldCheckForUpdate(newUpdateCheck))) {
                updateFutureCopy = getNestedExecutor().getNestedExecutor().submit(new Runnable() {
                    @Override
                    public void run() {
                        try (RetryScopes.Registration inheritedScopes = RetryScopes.install(outerRetryDepth)) {
                            WaitForGraph.taskStarted(graphKey, submittingThread);
                            try {
                                maybeUpdateRetryingOutsideWriteLock(newUpdateCheck);
                            } finally {
                                WaitForGraph.taskFinished(graphKey);
                            }
                        }
                    }
                });
                updateFuture = updateFutureCopy;
                reason = "started";
            } else {
                reason = "in progress";
            }
            if (refuseDeadlockingJoin(graphKey, "started".equals(reason))) {
                //pure joiner of an in-progress task that chains back onto this thread: stop waiting this attempt and
                //continue on the last published (complete) state; the submitter keeps driving the task
                return false;
            }
            joinUpdateFuture(graphKey, updateFutureCopy, reason);
            return true;
        } finally {
            WaitForGraph.lockReleased(graphKey);
            updateLock.unlock();
        }
    }

    /**
     * How many read holds this thread has on the key's table right now; they are released so the update this demand is
     * waiting for can take the table write lock, and taken back by {@link #restoreOwnTableReadHolds(int)} afterwards.
     * Zero when this thread is the key's current writer (a nested demand from inside its own attempt: there is nothing
     * to yield, the writer already owns the table).
     */
    private int parkOwnTableReadHolds() {
        final IReentrantReadWriteLock tableLock = getTable().getTableLock(getKey());
        if (tableLock.isWriteLockedByCurrentThread()) {
            return 0;
        }
        final int readHoldCount = tableLock.getReadHoldCount();
        if (readHoldCount <= 0) {
            return 0;
        }
        final ILock readLock = tableLock.readLock();
        for (int i = 0; i < readHoldCount; i++) {
            readLock.unlock();
        }
        return readHoldCount;
    }

    private void restoreOwnTableReadHolds(final int readHoldCount) {
        if (readHoldCount <= 0) {
            return;
        }
        final ILock readLock = getTable().getTableLock(getKey()).readLock();
        for (int i = 0; i < readHoldCount; i++) {
            readLock.lock();
        }
    }

    /**
     * The update attempt loop of the submitted task: every attempt takes the key's table write lock, runs the update
     * body once and releases the lock again BEFORE any waiting happens, so a failing updater never sleeps with the
     * table locked. Demand readers of that key take the table read side with a zero-wait tryLock, so an updater
     * backing off with the write lock held does not only delay itself - it refuses every reader of the table for the
     * whole sleep, and a source that keeps refusing (a frontier that never advances) keeps refusing them for as long as
     * it keeps retrying. With the lock released between attempts the back-off window is a window for readers, and the
     * next attempt resumes from the published high-water of the previous one (segment boundaries are publish points),
     * so a re-attempt costs the unpublished tail only.
     *
     * <p>
     * The loop is a fall-through site: when the submitting demand already runs inside an outer retry, a retryable
     * failure propagates to it after one attempt (that retry re-submits and re-attempts the whole operation), exactly
     * as the single-attempt task body did before; without an enclosing outer retry this loop IS the retry, waiting with
     * the table free. A consumer's own retry annotation on its update body that does NOT declare fall-through keeps
     * retrying inside its own call - under the write lock - and has to opt into this loop instead.
     */
    private void maybeUpdateRetryingOutsideWriteLock(final FDate newUpdateCheck) {
        //bounded probe of the table write lock before the first attempt: the skip-if-readers verdict must reach the
        //caller as it always did - before any attempt and before any back-off of this loop is entered. It is a QUEUED
        //wait rather than a see-if-free test because the readers holding the read side are very often the demand
        //readers of this very key: a zero-wait probe never joins the queue, and a non-fair read-write lock lets new
        //readers barge past a writer that is not queued, so a store under continuous demand-read pressure is never
        //allowed to advance - the writers' own readers keep refusing them (blocker-5 convoy)
        final ILock probedWriteLock = getTable().getTableLock(getKey()).writeLock();
        final int parkedReadHolds = parkOwnTableReadHolds();
        final boolean probed;
        try {
            probed = tryLockWriteLock(probedWriteLock);
        } finally {
            restoreOwnTableReadHolds(parkedReadHolds);
        }
        if (!probed) {
            throw newUpdateSkippedForActiveIterators();
        }
        probedWriteLock.unlock();
        new ARetryCallable<Void>(new RetryOriginator(this, "innerMaybeUpdate", getKey())) {

            @Override
            protected Void callRetry() {
                runUpdateAttemptUnderWriteLock(newUpdateCheck);
                return null;
            }

            @Override
            protected boolean isFallthrough() {
                //this loop is the retry of the task body: a site inside the attempt that declines to retry in place
                //because an outer retry already covers it hands the attempt to THIS loop (see Retry#fallthrough)
                return true;
            }

            @Override
            protected BackOffPolicy getBackOffPolicyOverride() {
                return newUpdateAttemptBackOffPolicy();
            }
        }.call();
    }

    /**
     * One update attempt: acquire the key's table write lock, run the update body, release the lock again. A failure
     * always leaves the table unlocked, so the loop's back-off happens while readers of the key are free to land.
     */
    private void runUpdateAttemptUnderWriteLock(final FDate newUpdateCheck) {
        final ILock writeLock = getTable().getTableLock(getKey()).writeLock();
        //this thread's own read holds must never be the reason its own attempt is refused (a pool thread that is
        //parked in an update task can also be the one holding an open iterator of the same key, and a read hold that
        //belongs to the writer is not a foreign iterator) - same release/re-lock pair as the demand side and as
        //ATimeSeriesUpdater.update()
        final int parkedReadHolds = parkOwnTableReadHolds();
        try {
            if (!tryLockWriteLock(writeLock)) {
                //don't try to update if a backtest is running which is keeping iterators open - but do NOT let the
                //caller proceed believing the update happened: it would read interior holes that the still-running
                //update of another context will fill later, making the run's result depend on thread scheduling (cold
                //vs warm drift in the feed's gap-close counters). Surface it as a retry so the demand read waits until
                //the iterators close and the data actually lands.
                throw newUpdateSkippedForActiveIterators();
            }
            try {
                getTable().getLookupTableCache(getKey()).maybeUpdateIndex();
                innerMaybeUpdate(getKey());
                LazyDataUpdaterProperties.maybeUpdateFinished(getUpdaterId());
                //update timestamp only at the end if successful
                lastUpdateCheck = newUpdateCheck;
                lastResetIndex = getTable().getLastResetIndex();
            } finally {
                writeLock.unlock();
            }
        } finally {
            restoreOwnTableReadHolds(parkedReadHolds);
        }
    }

    /**
     * Takes the key's table write lock with the bounded, queued wait of an update attempt; false when the read side
     * stayed busy past that window.
     */
    private boolean tryLockWriteLock(final ILock writeLock) {
        try {
            return writeLock.tryLock(newUpdateAttemptWriteLockWait());
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * How long one update attempt waits for the key's table write lock before it declares the table busy. Queued, so a
     * writer is not starved by readers barging past it (that is what the zero-wait probe used to do), and bounded, so
     * a table that is genuinely being iterated by another run still gets the skip verdict instead of convoying the
     * whole demand side behind it: keep it below the demand-side retry ceilings. {@code ATimeSeriesUpdater.update()}
     * applies the same policy for its own acquisition of this lock (ACQUIRE_WRITE_LOCK_TIMEOUT).
     */
    protected Duration newUpdateAttemptWriteLockWait() {
        return TimeSeriesProperties.UPDATE_ATTEMPT_ACQUIRE_WRITE_LOCK_TIMEOUT;
    }

    private NonBlockingRetryLaterRuntimeException newUpdateSkippedForActiveIterators() {
        final IReentrantReadWriteLock tableLock = getTable().getTableLock(getKey());
        //the verdict used to assert "of another run" without ever looking at who holds the read side - the holders are
        //now reported, because who holds it decides whether this is a foreign backtest iterator (legitimate yield) or
        //the demanding readers themselves (the blocker-5 convoy, fixed by the demand parking its own iterators)
        final String holders = "readHolders[" + tableLock.getReadLockCount() + "], ownReadHolds["
                + tableLock.getReadHoldCount() + "], queuedThreads[" + tableLock.getQueueLength() + "]";
        return new FastNonBlockingRetryLaterRuntimeException(
                ALazyDataUpdater.class.getSimpleName() + ".maybeUpdate: update of " + getElementsName() + " ["
                        + getKeyStr() + "] skipped because the table write lock is held by active iterators past ["
                        + newUpdateAttemptWriteLockWait() + "] (" + holders
                        + ") - retrying instead of reading an incomplete store");
    }

    /**
     * The schedule the update attempt loop sleeps on between attempts. It is always applied with the table write lock
     * RELEASED (the loop lives outside the locked region), which is the whole point of the restructure. The default is
     * the exponential schedule the retry aspect of an update body used: 1 ms growing to one minute.
     */
    protected BackOffPolicy newUpdateAttemptBackOffPolicy() {
        return BackOffPolicies.exponentialBackOff(UPDATE_ATTEMPT_BACK_OFF_MIN, UPDATE_ATTEMPT_BACK_OFF_MAX);
    }

    private void joinUpdateFuture(final String graphKey, final Future<?> updateFutureCopy, final String reason) {
        WaitForGraph.beginJoinWait(graphKey);
        try {
            if (DatabaseThreads.isThreadBlockingUpdateDatabaseDisabled()) {
                try {
                    Futures.waitNoInterrupt(updateFutureCopy,
                            IntegrationProperties.NON_BLOCKING_ASYNC_WAIT_TIMEOUT);
                    updateFuture = null;
                } catch (final TimeoutException e) {
                    throw new FastNonBlockingRetryLaterRuntimeException(
                            ALazyDataUpdater.class.getSimpleName()
                                    + ".maybeUpdate: async update " + reason
                                    + " while operating in non-blocking mode for " + getElementsName() + ": "
                                    + key,
                            e);
                }
            } else {
                //unbounded join by design: cycle edges are refused up front and the task itself aborts retryably on cyclic pin waits (see ATimeSeriesUpdater.trackRemoteUpdate), so a legitimate long-running update is waited out patiently
                Futures.waitNoInterrupt(updateFutureCopy);
                updateFuture = null;
            }
        } finally {
            WaitForGraph.endJoinWait(graphKey);
        }
    }

    /**
     * Caller-side cycle handling: in-tree cycles adopt the consistent pinned state (skip update this attempt),
     * foreign cycles are refused retryably. Returns true when the caller must not proceed with the update wait.
     */
    /**
     * Re-entrant demand for a key THIS very task is already rebuilding: joining its own future or adopting its
     * pass state would deadlock or read unpublished partial values - skip the update for this demand; the values
     * arrive with the in-flight pass (and the forced re-epoch sentinel covers reset passes), so this is a
     * scheduling decision, not a value decision (see ANALYSIS-precalc-fixedpoint).
     */
    private boolean skipSelfServedDemand(final String graphKey) {
        if (WaitForGraph.isSelfServedKey(graphKey)) {
            log.warn("Skipping re-entrant self-served update demand of [%s] for [%s]", getElementsName(), getKeyStr());
            return true;
        }
        return false;
    }

    private boolean skipCycleForCaller(final String graphKey) {
        if (!WaitForGraph.wouldCloseCycle(graphKey)) {
            return false;
        }
        if (WaitForGraph.isCycleOwnedByOwnDemandTree(graphKey)) {
            //The pin blocking this update is held by our own demand tree (a submitter ancestor is iterating the pinned, already-complete version): that version is exactly the data the tree is consuming, so reading it is tree-consistent - skip the update instead of aborting (the store cannot change while pinned; this is what makes mutually nested precalc chains converge instead of retry-spinning)
            log.warn("Skipping update of [%s] for [%s] this attempt: the blocking heartbeat pin is held inside our own demand tree, reading the consistent in-tree state - graph: %s", getElementsName(), getKeyStr(), WaitForGraph.describeCycle(graphKey));
            return true;
        }
        if (!WaitForGraph.shouldFallThroughAsCycleVictim(graphKey)) {
            //foreign cycle, but this is NOT the designated victim edge (deterministic max-key tie-break): the
            //participant wanting the greatest key will abort and unwind its pins - this wait resolves, proceed
            return false;
        }
        log.warn("Falling through as cycle victim for update of [%s] for [%s]: deterministic max-key yield to unwind and let the cycle complete serially: %s", getElementsName(), getKeyStr(), WaitForGraph.describeCycle(graphKey));
        throw new FastNonBlockingRetryLaterRuntimeException(
                ALazyDataUpdater.class.getSimpleName() + ".maybeUpdate: update of "
                        + getElementsName() + " [" + getKeyStr()
                        + "] aborted retryably because this thread's wait would close a wait-for cycle: "
                        + WaitForGraph.describeCycle(graphKey));
    }

    private boolean refuseDeadlockingJoin(final String graphKey, final boolean submitterHere) {
        //the task serving this key is waiting (directly or transitively) for a heartbeat pin THIS thread holds open: joining it would deadlock our pins closed. Abandon the join retryably - unwinding now closes our pins so the task can proceed, and the demand loop re-attempts cleanly afterwards
        if (WaitForGraph.joinWouldDeadlockOwnPins(graphKey)) {
            if (!submitterHere) {
                log.warn("Not joining update of [%s] for [%s] this attempt: the running task chains back onto this thread - skipping the wait and continuing on the last published state (submitter keeps driving it)", getElementsName(), getKeyStr());
                return true;
            }
            log.warn("Aborting join on update of [%s] for [%s]: the running task chains back onto a heartbeat pin held by this very thread - unwinding to release our pins and retry later", getElementsName(), getKeyStr());
            throw new FastNonBlockingRetryLaterRuntimeException(
                    ALazyDataUpdater.class.getSimpleName() + ".maybeUpdate: joining the update of "
                            + getElementsName() + " [" + getKeyStr()
                            + "] would deadlock pins held by this thread - retrying after our pins close");
        }
        return false;
    }

    protected <T> void logReload(final boolean logged, final String name, final T oldValue, final String reason,
            final T newValue) {
        if (!logged) {
            logReload(name, oldValue, reason, newValue);
        }
    }

    protected <T> void logReload(final String name, final T oldValue, final String reason, final T newValue) {
        log.warn("Updating [%s] after reset because existing %s [%s] %s [%s]", this, name, oldValue, reason, newValue);
    }

    protected <T> void logUpdate(final boolean logged, final String name, final T oldValue, final String reason,
            final T newValue) {
        if (!logged) {
            logUpdate(name, oldValue, reason, newValue);
        }
    }

    protected <T> void logUpdate(final String name, final T oldValue, final String reason, final T newValue) {
        log.warn("Updating [%s] because existing %s [%s] %s [%s]", this, name, oldValue, reason, newValue);
    }

    protected boolean shouldSkipUpdateOnCurrentThreadIfAlreadyRunning() {
        return isSkipUpdateOnCurrentThreadIfAlreadyRunning();
    }

    public boolean shouldCheckForUpdate() {
        return shouldCheckForUpdate(FDate.now());
    }

    protected boolean shouldCheckForUpdate(final FDate curTime) {
        return !FDates.isSameJulianDay(lastUpdateCheck, curTime) || lastResetIndex != getTable().getLastResetIndex();
    }

    protected final TimeSeriesUpdaterResult doUpdate(final FDate estimatedTo)
            throws IncompleteUpdateRetryableException {
        if (estimatedTo == null) {
            throw new NullPointerException("estimatedTo should not be null");
        }
        try {
            final ALoggingTimeSeriesUpdater<K, V> updater = new ALoggingTimeSeriesUpdater<K, V>(key, getTable(), log) {

                @Override
                protected FDate extractStartTime(final V element) {
                    return ALazyDataUpdater.this.extractStartTime(element);
                }

                @Override
                protected FDate extractEndTime(final V element) {
                    return ALazyDataUpdater.this.extractEndTime(element);
                }

                @Override
                protected ICloseableIterable<? extends V> getSource(final FDate updateFrom) {
                    final ICloseableIterable<? extends V> downloadElements = ALazyDataUpdater.this
                            .downloadElements(getKey(), updateFrom);
                    return downloadElements;
                }

                @Override
                protected String getElementsName() {
                    return ALazyDataUpdater.this.getElementsName();
                }

                @Override
                protected ITimeSeriesBoundaryCommitListener newBoundaryCommitListener() {
                    return ALazyDataUpdater.this.newBoundaryCommitListener();
                }

                @Override
                public Percent getProgress(final FDate minTime, final FDate maxTime) {
                    if (minTime == null) {
                        return null;
                    }
                    if (maxTime == null) {
                        return null;
                    }
                    return new Percent(new Duration(minTime, maxTime), new Duration(minTime, estimatedTo))
                            .orLower(Percent.ONE_HUNDRED_PERCENT);
                }

            };
            final Callable<TimeSeriesUpdaterResult> task = new Callable<TimeSeriesUpdaterResult>() {
                @Override
                public TimeSeriesUpdaterResult call() throws Exception {
                    final TimeSeriesUpdaterResult result = updater.update();
                    final FDate updatedTo = result.getUpdatedTo();
                    if (updatedTo != null) {
                        final Duration timegap = new Duration(updatedTo, estimatedTo);
                        if (timegap.isGreaterThan(Duration.ONE_YEAR)) {
                            //might be a race condition in parallel writes that aborts after the first 10k elements chunk
                            log.error(getTable().hashKeyToString(getKey())
                                    + ": Potential problem with data updates: maxTime[" + updatedTo
                                    + "] is too far away from estimatedTo[" + estimatedTo + "]: " + timegap + " > "
                                    + Duration.ONE_YEAR);
                        }
                    }
                    return result;
                }
            };
            final String taskName = "Loading " + getElementsName() + " for " + getKeyStr();
            final Callable<Percent> progress = newProgressCallable(estimatedTo, updater);
            final TimeSeriesUpdaterResult result = TaskInfoCallable.of(taskName, task, progress).call();
            try {
                LazyDataUpdaterProperties.setLastUpdateTo(getUpdaterId(), FDates.max(estimatedTo, result.getUpdatedTo()));
                return result;
            } catch (final Throwable t) {
                //the result (with its live heartbeat pin and the name-keyed thread lock slot) must not depend on GC
                //finalization: a throw in this hand-over window would otherwise orphan it - on a quiet heap the
                //warning-finalizer can take arbitrarily long, and the idle pool thread keeps holding the thread
                //lock, blocking every later attempt for this key (zombie claim forensics, smoke-41/43)
                result.close();
                throw t;
            }
        } catch (final IncompleteUpdateRetryableException e) {
            throw e;
        } catch (final Throwable e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * The boundary-commit notification attached to every update transaction this updater's {@code doUpdate} creates
     * (null = silent, the default): each segment boundary the attempt publishes reports its new frontier, so a
     * consumer that mirrors the store's downloaded first/last date keys can advance them with published segments
     * instead of only after a complete update.
     */
    protected ITimeSeriesBoundaryCommitListener newBoundaryCommitListener() {
        return null;
    }

    protected Callable<Percent> newProgressCallable(final FDate estimatedTo,
            final ALoggingTimeSeriesUpdater<K, V> updater) {
        if (estimatedTo == null) {
            return null;
        }
        return new Callable<Percent>() {
            @Override
            public Percent call() throws Exception {
                return updater.getProgress();
            }
        };
    }

    protected abstract ATimeSeriesDB<K, V> getTable();

    protected abstract ICloseableIterable<? extends V> downloadElements(K key, FDate fromDate);

    protected abstract String getElementsName();

    protected abstract FDate extractStartTime(V element);

    protected abstract FDate extractEndTime(V element);

    protected abstract void innerMaybeUpdate(K key);

    private final class NonBlockingMaybeUpdateForce extends ANonBlockingRunnable {
        private NonBlockingMaybeUpdateForce(final Class<?> parentClass, final String parentInfo) {
            super(parentClass, parentInfo);
        }

        @Override
        public void run() {
            maybeUpdate(true);
        }
    }

    private final class NonBlockingMaybeUpdate extends ANonBlockingRunnable {
        private NonBlockingMaybeUpdate(final Class<?> parentClass, final String parentInfo) {
            super(parentClass, parentInfo);
        }

        @Override
        public void run() {
            maybeUpdate();
        }
    }

}