package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.util.concurrent.Callable;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang3.BooleanUtils;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.integration.concurrent.DatabaseThreads;
import de.invesdwin.context.integration.concurrent.nonblocking.ANonBlockingRunnable;
import de.invesdwin.context.integration.concurrent.nonblocking.DisabledNonBlockingRunnable;
import de.invesdwin.context.integration.concurrent.nonblocking.INonBlockingRunnable;
import de.invesdwin.context.integration.retry.NonBlockingRetryLaterRuntimeException;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.concurrent.graph.WaitForGraph;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.IReentrantLock;
import de.invesdwin.util.concurrent.lock.Locks;
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

    private static final FastThreadLocal<Boolean> SKIP_UPDATE_ON_CURRENT_THREAD_IF_ALREADY_RUNNING = new FastThreadLocal<Boolean>();

    //shares the in-memory update lock between updater instances targeting the SAME physical store key (updaterId embeds the absolute shared version directory path): two contexts racing to update one store is what produced the heartbeat-pin-vs-join cross edges in the precalculated livelock (and it fragments the wait-for graph with multiple owners per key). Single-flight per key inside this JVM removes that class at its source; other JVMs stay serialised by the heartbeat file lock.
    private static final Map<String, IReentrantLock> SHARED_UPDATE_LOCKS = ILockCollectionFactory.getInstance(true)
            .newConcurrentMap();
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
            updateLock = SHARED_UPDATE_LOCKS.computeIfAbsent(getUpdaterId() + "_updateLock",
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
            //Deadlock avoidance: never add a wait edge that closes a wait-for cycle inside this JVM (the permanent precalculated livelock formed exactly like that). If the target update's progress transitively depends on a lock, pin, or task held by this very thread, abort this attempt retryably; later attempts run once the chain has unwound
            if (skipCycleForCaller(graphKey)) {
                return false;
            }
            final IReentrantLock updateLock = getUpdateLock();
            if (shouldSkipUpdateOnCurrentThreadIfAlreadyRunning()) {
                try {
                    if (!updateLock.tryLock(TimeSeriesProperties.ACQUIRE_UPDATE_LOCK_TIMEOUT)) {
                        //do NOT let the caller proceed believing the update happened when another context holds the update lock past the timeout: that reads interior holes the pending update fills later, making results depend on thread scheduling - surface it as a retry instead (same policy as the table write-lock case below)
                        throw new NonBlockingRetryLaterRuntimeException(
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
                if (updateFutureCopy == null
                        || updateFutureCopy.isDone() && (force || shouldCheckForUpdate(newUpdateCheck))) {
                    updateFutureCopy = getNestedExecutor().getNestedExecutor().submit(new Runnable() {
                        @Override
                        public void run() {
                            WaitForGraph.taskStarted(graphKey, submittingThread);
                            try {
                                final ILock writeLock = getTable().getTableLock(key).writeLock();
                                if (!writeLock.tryLock()) {
                                    //don't try to update if currently a backtest is running which is keeping iterators
                                    //open - but do NOT let the caller proceed believing the update happened: it would
                                    //read interior holes that the still-running update of another context will fill
                                    //later, making the run's result depend on thread scheduling (cold vs warm drift in
                                    //the feed's gap-close counters). Surface it as a retry so the demand read waits
                                    //until the iterators close and the data actually lands.
                                    throw new NonBlockingRetryLaterRuntimeException(
                                            ALazyDataUpdater.class.getSimpleName() + ".maybeUpdate: update of "
                                                    + getElementsName() + " [" + getKeyStr()
                                                    + "] skipped because the table write lock is held by active"
                                                    + " iterators of another run - retrying instead of reading an"
                                                    + " incomplete store");
                                }
                                try {
                                    getTable().getLookupTableCache(key).maybeUpdateIndex();
                                    innerMaybeUpdate(key);
                                    LazyDataUpdaterProperties.maybeUpdateFinished(getUpdaterId());
                                    //update timestamp only at the end if successful
                                    lastUpdateCheck = newUpdateCheck;
                                    lastResetIndex = getTable().getLastResetIndex();
                                } finally {
                                    writeLock.unlock();
                                }
                            } finally {
                                WaitForGraph.taskFinished(graphKey);
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
                WaitForGraph.beginJoinWait(graphKey);
                try {
                    if (DatabaseThreads.isThreadBlockingUpdateDatabaseDisabled()) {
                        try {
                            Futures.waitNoInterrupt(updateFutureCopy,
                                    IntegrationProperties.NON_BLOCKING_ASYNC_WAIT_TIMEOUT);
                            updateFuture = null;
                        } catch (final TimeoutException e) {
                            throw new NonBlockingRetryLaterRuntimeException(
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
                return true;
            } finally {
                WaitForGraph.lockReleased(graphKey);
                updateLock.unlock();
            }
        }
        return false;
    }

    /**
     * Caller-side cycle handling: in-tree cycles adopt the consistent pinned state (skip update this attempt),
     * foreign cycles are refused retryably. Returns true when the caller must not proceed with the update wait.
     */
    private boolean skipCycleForCaller(final String graphKey) {
        if (!WaitForGraph.wouldCloseCycle(graphKey)) {
            return false;
        }
        if (WaitForGraph.isCycleOwnedByOwnDemandTree(graphKey)) {
            //The pin blocking this update is held by our own demand tree (a submitter ancestor is iterating the pinned, already-complete version): that version is exactly the data the tree is consuming, so reading it is tree-consistent - skip the update instead of aborting (the store cannot change while pinned; this is what makes mutually nested precalc chains converge instead of retry-spinning)
            log.warn("Skipping update of [%s] for [%s] this attempt: the blocking heartbeat pin is held inside our own demand tree, reading the consistent in-tree state - graph: %s", getElementsName(), getKeyStr(), WaitForGraph.describeCycle(graphKey));
            return true;
        }
        throw new NonBlockingRetryLaterRuntimeException(
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
            throw new NonBlockingRetryLaterRuntimeException(
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
            LazyDataUpdaterProperties.setLastUpdateTo(getUpdaterId(), FDates.max(estimatedTo, result.getUpdatedTo()));
            return result;
        } catch (final IncompleteUpdateRetryableException e) {
            throw e;
        } catch (final Throwable e) {
            throw Throwables.propagate(e);
        }
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