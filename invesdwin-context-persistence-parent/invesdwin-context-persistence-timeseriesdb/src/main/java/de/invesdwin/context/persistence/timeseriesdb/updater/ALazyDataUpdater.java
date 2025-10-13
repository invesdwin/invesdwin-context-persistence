package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang3.BooleanUtils;

import de.invesdwin.context.integration.DatabaseThreads;
import de.invesdwin.context.integration.retry.NonBlockingRetryLaterRuntimeException;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.concurrent.future.Futures;
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
    protected final Log log = new Log(this);
    private final K key;
    @GuardedBy("getUpdateLock()")
    private volatile FDate lastUpdateCheck = FDates.MIN_DATE;
    @GuardedBy("this")
    private IReentrantLock updateLock;
    @GuardedBy("this")
    private Future<?> updateFuture;
    @GuardedBy("none for performance")
    private String updaterId;

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
                + getTable().getDirectory().getAbsolutePath() + "_" + keyToString(key) + "_" + getElementsName();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(keyToString(key)).toString();
    }

    public K getKey() {
        return key;
    }

    private synchronized IReentrantLock getUpdateLock() {
        if (updateLock == null) {
            updateLock = Locks.newReentrantLock(getUpdaterId() + "_updateLock");
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
    public final void maybeUpdate(final boolean force) {
        final FDate newUpdateCheck = new FDate();
        if (force || shouldCheckForUpdate(newUpdateCheck)) {
            final IReentrantLock updateLock = getUpdateLock();
            if (shouldSkipUpdateOnCurrentThreadIfAlreadyRunning()) {
                try {
                    if (!updateLock.tryLock(TimeSeriesProperties.ACQUIRE_UPDATE_LOCK_TIMEOUT)) {
                        return;
                    }
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                updateLock.lock();
            }
            try {
                if (!force && !shouldCheckForUpdate(newUpdateCheck)) {
                    //some sort of double checked locking to skip if someone else came before us
                    return;
                }
                Future<?> updateFutureCopy = updateFuture;
                final String reason;
                if (updateFutureCopy == null
                        || updateFutureCopy.isDone() && (force || shouldCheckForUpdate(newUpdateCheck))) {
                    updateFutureCopy = getNestedExecutor().getNestedExecutor().submit(new Runnable() {
                        @Override
                        public void run() {
                            final ILock writeLock = getTable().getTableLock(key).writeLock();
                            if (!writeLock.tryLock()) {
                                //don't try to update if currently a backtest is running which is keeping iterators open
                                return;
                            }
                            try {
                                innerMaybeUpdate(key);
                                LazyDataUpdaterProperties.maybeUpdateFinished(getUpdaterId());
                                //update timestamp only at the end if successful
                                lastUpdateCheck = newUpdateCheck;
                            } finally {
                                writeLock.unlock();
                            }
                        }
                    });
                    updateFuture = updateFutureCopy;
                    reason = "started";
                } else {
                    reason = "is in progress";
                }
                if (DatabaseThreads.isThreadBlockingUpdateDatabaseDisabled()) {
                    try {
                        Futures.waitNoInterrupt(updateFutureCopy,
                                TimeSeriesProperties.NON_BLOCKING_ASYNC_UPDATE_WAIT_TIMEOUT);
                        updateFuture = null;
                    } catch (final TimeoutException e) {
                        throw new NonBlockingRetryLaterRuntimeException(
                                ALazyDataUpdater.class.getSimpleName() + ".maybeUpdate: async update " + reason
                                        + " while operating in non-blocking mode for " + getElementsName() + ": " + key,
                                e);
                    }
                } else {
                    Futures.waitNoInterrupt(updateFutureCopy);
                    updateFuture = null;
                }
            } finally {
                updateLock.unlock();
            }
        }
    }

    protected <T> void logReload(final boolean logged, final String name, final T oldValue, final String reason,
            final T newValue) {
        if (!logged) {
            logReload(name, oldValue, reason, newValue);
        }
    }

    protected <T> void logReload(final String name, final T oldValue, final String reason, final T newValue) {
        if (oldValue != null) {
            log.warn("Updating [%s] after reset because existing %s [%s] %s [%s]", this, name, oldValue, reason,
                    newValue);
        }
    }

    protected <T> void logUpdate(final boolean logged, final String name, final T oldValue, final String reason,
            final T newValue) {
        if (!logged) {
            logUpdate(name, oldValue, reason, newValue);
        }
    }

    protected <T> void logUpdate(final String name, final T oldValue, final String reason, final T newValue) {
        if (oldValue != null) {
            log.warn("Updating [%s] because existing %s [%s] %s [%s]", this, name, oldValue, reason, newValue);
        }
    }

    protected boolean shouldSkipUpdateOnCurrentThreadIfAlreadyRunning() {
        return isSkipUpdateOnCurrentThreadIfAlreadyRunning();
    }

    protected boolean shouldCheckForUpdate(final FDate curTime) {
        return !FDates.isSameJulianDay(lastUpdateCheck, curTime);
    }

    protected final FDate doUpdate(final FDate estimatedTo) throws IncompleteUpdateRetryableException {
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
                protected String keyToString(final K key) {
                    return ALazyDataUpdater.this.keyToString(key);
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
            final Callable<FDate> task = new Callable<FDate>() {
                @Override
                public FDate call() throws Exception {
                    updater.update();
                    final FDate maxTime = updater.getMaxTime();
                    if (maxTime != null) {
                        final Duration timegap = new Duration(maxTime, estimatedTo);
                        if (timegap.isGreaterThan(Duration.ONE_YEAR)) {
                            //might be a race condition in parallel writes that aborts after the first 10k elements chunk
                            log.error(getTable().hashKeyToString(getKey())
                                    + ": Potential problem with data updates: maxTime[" + maxTime
                                    + "] is too far away from estimatedTo[" + estimatedTo + "]: " + timegap + " > "
                                    + Duration.ONE_YEAR);
                        }
                    }
                    return maxTime;
                }
            };
            final String taskName = "Loading " + getElementsName() + " for " + keyToString(getKey());
            final Callable<Percent> progress = newProgressCallable(estimatedTo, updater);
            final FDate updatedTo = TaskInfoCallable.of(taskName, task, progress).call();
            LazyDataUpdaterProperties.setLastUpdateTo(getUpdaterId(), FDates.max(estimatedTo, updatedTo));
            return updatedTo;
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

    protected abstract String keyToString(K key);

    protected abstract String getElementsName();

    protected abstract FDate extractStartTime(V element);

    protected abstract FDate extractEndTime(V element);

    protected abstract void innerMaybeUpdate(K key);

    public void reset() {
        lastUpdateCheck = FDates.MIN_DATE;
    }

}