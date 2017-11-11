package de.invesdwin.context.persistence.leveldb.timeseries;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang3.BooleanUtils;

import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.loadingcache.historical.refresh.HistoricalCacheRefreshManager;
import de.invesdwin.util.concurrent.ANestedExecutor;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.Futures;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.lang.ProcessedEventsRateString;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FDates;
import io.netty.util.concurrent.FastThreadLocal;

@ThreadSafe
public abstract class ADataUpdater<K, V> {

    private static final FastThreadLocal<Boolean> SKIP_UPDATE_ON_CURRENT_THREAD_IF_ALREADY_RUNNING = new FastThreadLocal<Boolean>();
    private static final ANestedExecutor NESTED_EXECUTOR = new ANestedExecutor(
            ADataUpdater.class.getSimpleName() + "_runningUpdate") {
        @Override
        protected WrappedExecutorService newNestedExecutor(final String nestedName) {
            return Executors.newFixedThreadPool(nestedName, Executors.getCpuThreadPoolCount());
        }
    };
    protected final Log log = new Log(this);
    private final K key;
    @GuardedBy("updateLock.writeLock for writes, none for reads")
    private volatile FDate lastUpdateCheck = FDate.MIN_DATE;
    private final ReentrantLock updateLock = new ReentrantLock();

    public ADataUpdater(final K key) {
        if (key == null) {
            throw new NullPointerException("key should not be null");
        }
        this.key = key;
    }

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
            if (isSkipUpdateOnCurrentThreadIfAlreadyRunning()) {
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
                final Future<?> future = NESTED_EXECUTOR.getNestedExecutor().submit(new Runnable() {
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

    protected boolean shouldCheckForUpdate(final FDate curTime) {
        return !FDates.isSameJulianDay(lastUpdateCheck, curTime);
    }

    protected final FDate doUpdate() throws IncompleteUpdateFoundException {
        final ATimeSeriesUpdater<K, V> updater = new ATimeSeriesUpdater<K, V>(key, getTable()) {

            private static final int BATCH_LOG_INTERVAL = 100_000 / BATCH_FLUSH_INTERVAL;
            private Integer lastFlushIndex;
            private Duration flushDuration;
            private long flushElementCount;
            private FDate lastFlushMaxTime;

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
                final ICloseableIterable<? extends V> downloadElements = downloadElements(key, updateFrom);
                return downloadElements;
            }

            @Override
            protected void onUpdateStart() {
                log.info("Updating %s for [%s]", getElementsName(), keyToString(key));
            }

            @Override
            protected void onFlush(final int flushIndex, final Instant flushStart, final UpdateProgress progress) {
                lastFlushIndex = Integers.max(lastFlushIndex, flushIndex);
                if (flushDuration == null) {
                    flushDuration = flushStart.toDuration();
                } else {
                    flushDuration = flushDuration.add(flushStart.toDuration());
                }
                flushElementCount += progress.getCount();
                lastFlushMaxTime = FDates.max(lastFlushMaxTime, progress.getMaxTime());
                if (flushIndex % BATCH_LOG_INTERVAL == 0) {
                    logFlush();
                }
            }

            private void logFlush() {
                Assertions.assertThat(lastFlushIndex).isNotNull();

                log.info("Persisted %s. %s batch for [%s]. Reached time [%s]. Processed [%s] during %s", lastFlushIndex,
                        getElementsName(), keyToString(key), lastFlushMaxTime,
                        new ProcessedEventsRateString(flushElementCount, flushDuration), flushDuration);

                lastFlushIndex = null;
                flushDuration = null;
                flushElementCount = 0;
                lastFlushMaxTime = null;
            }

            @Override
            protected void onUpdateFinished(final Instant updateStart) {
                if (lastFlushIndex != null) {
                    logFlush();
                }
                log.info("Finished updating %s %s for [%s] from [%s] to [%s] after %s", getCount(), getElementsName(),
                        keyToString(key), getMinTime(), getMaxTime(), updateStart);
            }
        };
        updater.update();
        //let DelegateBar/TickCaches fetch more data now by clearing them
        HistoricalCacheRefreshManager.refresh();
        return updater.getMaxTime();
    }

    protected abstract ATimeSeriesDB<K, V> getTable();

    protected abstract ICloseableIterable<? extends V> downloadElements(K key, FDate fromDate);

    protected abstract String keyToString(K key);

    protected abstract String getElementsName();

    protected abstract FDate extractTime(V element);

    protected abstract FDate extractEndTime(V element);

    protected abstract void innerMaybeUpdate(K key);

}