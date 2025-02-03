package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang3.BooleanUtils;

import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.concurrent.Threads;
import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.IReentrantLock;
import de.invesdwin.util.concurrent.lock.Locks;
import de.invesdwin.util.concurrent.nested.ANestedExecutor;
import de.invesdwin.util.concurrent.taskinfo.provider.TaskInfoCallable;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.duration.Duration;
import io.netty.util.concurrent.FastThreadLocal;

@ThreadSafe
public abstract class ALazyDataUpdater<K, V> implements ILazyDataUpdater<K, V> {

    private static final FastThreadLocal<Boolean> SKIP_UPDATE_ON_CURRENT_THREAD_IF_ALREADY_RUNNING = new FastThreadLocal<Boolean>();
    protected final Log log = new Log(this);
    private final K key;
    @GuardedBy("getUpdateLock()")
    private volatile FDate lastUpdateCheck = FDates.MIN_DATE;
    @GuardedBy("this")
    private IReentrantLock updateLock;

    public ALazyDataUpdater(final K key) {
        if (key == null) {
            throw new NullPointerException("key should not be null");
        }
        this.key = key;
    }

    public K getKey() {
        return key;
    }

    private synchronized IReentrantLock getUpdateLock() {
        if (updateLock == null) {
            updateLock = Locks.newReentrantLock(
                    ALazyDataUpdater.class.getSimpleName() + "_" + getTable().getName() + "_" + key + "_updateLock");
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
            if (Threads.isThreadRetryDisabledDefault()) {
                //don't update from UI thread, finalizer or when already interrupted
                return;
            }
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
                final Future<?> future = getNestedExecutor().getNestedExecutor().submit(new Runnable() {
                    @Override
                    public void run() {
                        final ILock writeLock = getTable().getTableLock(key).writeLock();
                        if (!writeLock.tryLock()) {
                            //don't try to update if currently a backtest is running which is keeping iterators open
                            return;
                        }
                        try {
                            innerMaybeUpdate(key);
                        } finally {
                            //update timestamp only at the end
                            lastUpdateCheck = newUpdateCheck;
                            writeLock.unlock();
                        }
                    }

                });
                try {
                    Futures.wait(future);
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } finally {
                updateLock.unlock();
            }
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
            return TaskInfoCallable.of(taskName, task, progress).call();
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

}