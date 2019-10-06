package de.invesdwin.context.persistence.timeseries.timeseriesdb.updater;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang3.BooleanUtils;

import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.ATimeSeriesUpdater;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.IncompleteUpdateFoundException;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.concurrent.lock.IReentrantLock;
import de.invesdwin.util.concurrent.lock.Locks;
import de.invesdwin.util.concurrent.nested.ANestedExecutor;
import de.invesdwin.util.concurrent.taskinfo.TaskInfoManager;
import de.invesdwin.util.concurrent.taskinfo.provider.TaskInfoCallable;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FDates;
import io.netty.util.concurrent.FastThreadLocal;

@ThreadSafe
public abstract class ADataUpdater<K, V> {

    private static final FastThreadLocal<Boolean> SKIP_UPDATE_ON_CURRENT_THREAD_IF_ALREADY_RUNNING = new FastThreadLocal<Boolean>();
    protected final Log log = new Log(this);
    private final K key;
    @GuardedBy("getUpdateLock()")
    private volatile FDate lastUpdateCheck = FDate.MIN_DATE;
    @GuardedBy("this")
    private IReentrantLock updateLock;

    public ADataUpdater(final K key) {
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
                    ADataUpdater.class.getSimpleName() + "_" + getTable().getName() + "_" + key + "_updateLock");
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

    public final void maybeUpdate() {
        final FDate newUpdateCheck = new FDate();
        if (shouldCheckForUpdate(newUpdateCheck)) {
            final IReentrantLock updateLock = getUpdateLock();
            if (shouldSkipUpdateOnCurrentThreadIfAlreadyRunning()) {
                try {
                    if (!updateLock.tryLock(5, TimeUnit.SECONDS)) {
                        return;
                    }
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                updateLock.lock();
            }
            try {
                if (!shouldCheckForUpdate(newUpdateCheck)) {
                    //some sort of double checked locking to skip if someone else came before us
                    return;
                }
                final Future<?> future = getNestedExecutor().getNestedExecutor().submit(new Runnable() {
                    @Override
                    public void run() {
                        if (!getTable().getTableLock(key).writeLock().tryLock()) {
                            //don't try to update if currently a backtest is running which is keeping iterators open
                            return;
                        }
                        try {
                            innerMaybeUpdate(key);
                        } finally {
                            //update timestamp only at the end
                            lastUpdateCheck = newUpdateCheck;
                            getTable().getTableLock(key).writeLock().unlock();
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

    protected final FDate doUpdate(final FDate estimatedTo) throws IncompleteUpdateFoundException {
        final ALoggingTimeSeriesUpdater<K, V> updater = new ALoggingTimeSeriesUpdater<K, V>(key, getTable(), log) {

            @Override
            protected FDate extractTime(final V element) {
                return ADataUpdater.this.extractTime(element);
            }

            @Override
            protected FDate extractEndTime(final V element) {
                return ADataUpdater.this.extractEndTime(element);
            }

            @Override
            protected ICloseableIterable<? extends V> getSource(final FDate updateFrom) {
                final ICloseableIterable<? extends V> downloadElements = downloadElements(getKey(), updateFrom);
                return downloadElements;
            }

            @Override
            protected String keyToString(final K key) {
                return ADataUpdater.this.keyToString(key);
            }

            @Override
            protected String getElementsName() {
                return ADataUpdater.this.getElementsName();
            }

            @Override
            protected boolean shouldWriteInParallel() {
                return ADataUpdater.this.shouldWriteInParallel();
            }

            @Override
            public Percent getProgress() {
                if (estimatedTo == null) {
                    return null;
                }
                final FDate from = getMinTime();
                if (from == null) {
                    return null;
                }
                final FDate curTime = getMaxTime();
                if (curTime == null) {
                    return null;
                }
                return new Percent(new Duration(from, curTime), new Duration(from, estimatedTo))
                        .orLower(Percent.ONE_HUNDRED_PERCENT);
            }

        };
        String taskName = TaskInfoManager.getCurrentThreadTaskInfoName();
        if (taskName == null) {
            taskName = "Loading " + getElementsName() + " for " + keyToString(getKey());
        }
        final Callable<FDate> task = new Callable<FDate>() {
            @Override
            public FDate call() throws Exception {
                updater.update();
                return updater.getMaxTime();
            }
        };
        final Callable<Percent> progress = newProgressCallable(estimatedTo, updater);
        try {
            return TaskInfoCallable.of(taskName, task, progress).call();
        } catch (final IncompleteUpdateFoundException e) {
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

    protected boolean shouldWriteInParallel() {
        return ATimeSeriesUpdater.DEFAULT_SHOULD_WRITE_IN_PARALLEL;
    }

    protected abstract ATimeSeriesDB<K, V> getTable();

    protected abstract ICloseableIterable<? extends V> downloadElements(K key, FDate fromDate);

    protected abstract String keyToString(K key);

    protected abstract String getElementsName();

    protected abstract FDate extractTime(V element);

    protected abstract FDate extractEndTime(V element);

    protected abstract void innerMaybeUpdate(K key);

}