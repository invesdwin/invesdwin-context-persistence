package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import io.netty.util.concurrent.FastThreadLocal;

@ThreadSafe
public final class LazyDataUpdaterProperties {

    private static final String KEY_UPDATE_LIMIT_TO = "UPDATE_LIMIT_TO";
    private static final AtomicInteger ACTIVE_BACKTEST_RUNS_COUNT = new AtomicInteger();
    private static final FastThreadLocal<FDate> UPDATE_LIMIT_TO_BEFORE = new FastThreadLocal<FDate>();
    private static final Map<String, FDate> UPDATERID_LASTUPDATETO = new ConcurrentHashMap<>();

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
        return setUpdateLimitTo(new FDate().withoutTime());
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

}
