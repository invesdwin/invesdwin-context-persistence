package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.range.TimeRange;
import io.netty.util.concurrent.FastThreadLocal;

@ThreadSafe
public final class LazyDataUpdaterProperties {

    private static final String KEY_UPDATE_LIMIT_TO = "UPDATE_LIMIT_TO";
    private static final AtomicInteger ACTIVE_BACKTEST_RUNS_COUNT = new AtomicInteger();
    //latch depth of the outer run entries (one per increase/decrease pair, not per task): drives the recalculation stamp below
    private static final AtomicInteger ACTIVE_RUNS_LATCH_COUNT = new AtomicInteger();
    private static volatile FDate latchedRecalculationStamp;
    private static final FastThreadLocal<FDate> UPDATE_LIMIT_TO_BEFORE = new FastThreadLocal<FDate>();
    private static final Map<String, FDate> UPDATERID_LASTUPDATETO = ILockCollectionFactory.getInstance(true)
            .newConcurrentMap();

    private static final SystemProperties SYSTEM_PROPERTIES;
    static {
        SYSTEM_PROPERTIES = new SystemProperties(LazyDataUpdaterProperties.class);
    }

    private LazyDataUpdaterProperties() {}

    public static int getActiveBacktestRunsCount() {
        return ACTIVE_BACKTEST_RUNS_COUNT.get();
    }

    public static void increaseActiveBacktestRunsCount() {
        increaseActiveBacktestRunsCount(1);
    }

    public static synchronized void increaseActiveBacktestRunsCount(final int count) {
        increaseActiveBacktestRunsCount(count, null);
    }

    /**
     * Run entry with the run's closed time range: besides the UPDATE_LIMIT_TO handling this also latches the
     * recalculation stamp of the run (see {@link #recalculationStamp()}); only the outermost entry latches.
     */
    public static void increaseActiveBacktestRunsCount(final TimeRange runTimeRange) {
        increaseActiveBacktestRunsCount(1, runTimeRange);
    }

    public static synchronized void increaseActiveBacktestRunsCount(final int count, final TimeRange runTimeRange) {
        final int latchDepth = ACTIVE_RUNS_LATCH_COUNT.incrementAndGet();
        if (latchDepth == 1) {
            latchedRecalculationStamp = getRecalculationStampFromRange(runTimeRange);
        }
        final int before = ACTIVE_BACKTEST_RUNS_COUNT.getAndAdd(count);
        if (before == 0) {
            if (getUpdateLimitTo() == null) {
                UPDATE_LIMIT_TO_BEFORE.set(setUpdateLimitTo());
            }
        }
    }

    public static void decreaseActiveBacktestRunsCount() {
        decreaseActiveBacktestRunsCount(1);
    }

    public static synchronized void decreaseActiveBacktestRunsCount(final int count) {
        final int latchDepth = ACTIVE_RUNS_LATCH_COUNT.decrementAndGet();
        if (latchDepth <= 0) {
            ACTIVE_RUNS_LATCH_COUNT.set(0);
            latchedRecalculationStamp = null;
        }
        final int after = ACTIVE_BACKTEST_RUNS_COUNT.addAndGet(-count);
        if (after == 0) {
            final FDate updateLimitToBefore = UPDATE_LIMIT_TO_BEFORE.get();
            if (updateLimitToBefore != null) {
                setUpdateLimitTo(updateLimitToBefore);
                UPDATE_LIMIT_TO_BEFORE.remove();
                //reset update info to allow full update again next time
                UPDATERID_LASTUPDATETO.clear();
            }
        }
    }

    public static FDate getLastUpdateTo(final String updaterId) {
        return UPDATERID_LASTUPDATETO.get(updaterId);
    }

    public static void setLastUpdateTo(final String updaterId, final FDate lastUpdateTo) {
        UPDATERID_LASTUPDATETO.put(updaterId, lastUpdateTo);
    }

    public static FDate getUpdateLimitTo(final String updaterId) {
        final FDate updateLimitTo = getUpdateLimitTo();
        final FDate lastUpdateTo = getLastUpdateTo(updaterId);
        return FDates.min(updateLimitTo, lastUpdateTo);
    }

    public static FDate getUpdateLimitTo() {
        return SYSTEM_PROPERTIES.getDateOptional(KEY_UPDATE_LIMIT_TO);
    }

    public static FDate setUpdateLimitTo(final FDate updateLimitTo) {
        final FDate before = getUpdateLimitTo();
        if (FDates.MAX_DATE.equals(updateLimitTo)) {
            SYSTEM_PROPERTIES.setDate(KEY_UPDATE_LIMIT_TO, null);
        } else {
            SYSTEM_PROPERTIES.setDate(KEY_UPDATE_LIMIT_TO, updateLimitTo);
        }
        if (before == null) {
            return FDates.MAX_DATE;
        } else {
            return before;
        }
    }

    public static FDate setUpdateLimitTo() {
        return setUpdateLimitTo(FDate.now().withoutTime());
    }

    /**
     * Only when no backtest is in progress that potentially uses the given data a reset is allowed. Otherwise the reset
     * will delete the underlying data which will cause the backtest to fail.
     */
    public static boolean isRecalculationAllowed(final String updaterId) {
        final FDate updateLimitTo = getUpdateLimitTo();
        if (updateLimitTo == null) {
            //let it update as much as it wants
            return true;
        }
        final FDate lastUpdateTo = getLastUpdateTo(updaterId);
        if (lastUpdateTo == null) {
            //let it update as much as it wants
            return true;
        }
        //wait for backtests to finish before checking recalculation again
        return false;
    }

    public static void maybeUpdateFinished(final String updaterId) {
        final FDate updateLimitTo = LazyDataUpdaterProperties.getUpdateLimitTo();
        if (updateLimitTo == null) {
            //no backtests running, don't remember that this updater ran without updating
            return;
        }
        final FDate lastUpdateTo = LazyDataUpdaterProperties.getLastUpdateTo(updaterId);
        if (lastUpdateTo != null) {
            //updater updated, thus no need to replace lastUpdateTo
            return;
        }
        //updater did not update, add a marker so that updater does not run again while backtests run
        LazyDataUpdaterProperties.setLastUpdateTo(updaterId, updateLimitTo);
    }

    /**
     * The recalculation stamp of the running context: the date stamped into derived table recalculation
     * metadata (calculated ticks, bars, schema values, exchange rates) and the as-of date at which bars
     * become the current unfinished ones of the run. While a bounded run is active it is the latched end of
     * that run's closed time window, so the stamp becomes a property of the replayed window and not of the
     * machine clock - equal for every run over the same window; while no stamp is latched, the value already
     * latched as UPDATE_LIMIT_TO by the run-count lifecycle is used, and only if that is absent as well does
     * this fall back to the wall clock.
     */
    public static FDate recalculationStamp() {
        final FDate latchedStamp = latchedRecalculationStamp;
        if (latchedStamp != null) {
            return latchedStamp;
        }
        final FDate updateLimitTo = getUpdateLimitTo();
        if (updateLimitTo != null) {
            return updateLimitTo;
        }
        return FDate.now();
    }

    /**
     * True once no sibling run is latched any more: the latch depth is the number of run entries currently
     * active, so a depth greater than one means another bounded run may still be reading the shared derived
     * tables. A depth of zero (nothing latched, e.g. a live session or a cleanup after all runs finished)
     * also counts as the last one.
     */
    public static boolean isLastActiveBoundedRun() {
        return ACTIVE_RUNS_LATCH_COUNT.get() <= 1;
    }

    /**
     * A run that does not close its time range latches no recalculation stamp and keeps the fallback
     * described in {@link #recalculationStamp()}.
     */
    private static FDate getRecalculationStampFromRange(final TimeRange timeRange) {
        if (timeRange == null || timeRange.isUnlimited()) {
            return null;
        }
        final FDate to = timeRange.getTo();
        if (to == null || FDates.MAX_DATE.equals(to)) {
            return null;
        }
        return to;
    }

}
