package de.invesdwin.context.persistence.timeseriesdb.updater;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.range.TimeRange;

/**
 * The run as-of date latch driven by the run-count lifecycle of {@link LazyDataUpdaterProperties}: the
 * outermost run entry latches the end of its closed time range, nested entries keep it, the last closer drops
 * it; with nothing latched the fallback chain is UPDATE_LIMIT_TO first and the wall clock only second.
 */
@NotThreadSafe
public class LazyDataUpdaterPropertiesBoundedRunTest {

    private static final FDate WINDOW_TO = FDateBuilder.newDate(2026, 10, 31);
    private static final FDate UPDATE_LIMIT_TO = FDateBuilder.newDate(2000, 1, 1);

    @Test
    public void testBoundedRunAsOfDateLatch() {
        try {
            //nothing latched, UPDATE_LIMIT_TO set: runAsOfDate falls back to it
            LazyDataUpdaterProperties.setUpdateLimitTo(UPDATE_LIMIT_TO);
            Assertions.assertThat(LazyDataUpdaterProperties.isLastActiveBoundedRun()).isTrue();
            Assertions.assertThat(LazyDataUpdaterProperties.runAsOfDate()).isEqualTo(UPDATE_LIMIT_TO);
            //outer bounded run entry: latches the end of the closed run window
            LazyDataUpdaterProperties.increaseActiveBacktestRunsCount(
                    new TimeRange(FDates.MIN_DATE, WINDOW_TO));
            Assertions.assertThat(LazyDataUpdaterProperties.getActiveBacktestRunsCount()).isEqualTo(1);
            Assertions.assertThat(LazyDataUpdaterProperties.runAsOfDate()).isEqualTo(WINDOW_TO);
            Assertions.assertThat(LazyDataUpdaterProperties.isLastActiveBoundedRun()).isTrue();
            //nested entry with a task count and an open range: outer latch stays, sibling is active
            LazyDataUpdaterProperties.increaseActiveBacktestRunsCount(3, TimeRange.UNLIMITED);
            Assertions.assertThat(LazyDataUpdaterProperties.getActiveBacktestRunsCount()).isEqualTo(4);
            Assertions.assertThat(LazyDataUpdaterProperties.runAsOfDate()).isEqualTo(WINDOW_TO);
            Assertions.assertThat(LazyDataUpdaterProperties.isLastActiveBoundedRun()).isFalse();
            //the nested entry is gone: last closer again
            LazyDataUpdaterProperties.decreaseActiveBacktestRunsCount(3);
            Assertions.assertThat(LazyDataUpdaterProperties.isLastActiveBoundedRun()).isTrue();
            //after the last run finished: latch dropped, back to the UPDATE_LIMIT_TO fallback
            LazyDataUpdaterProperties.decreaseActiveBacktestRunsCount();
            Assertions.assertThat(LazyDataUpdaterProperties.getActiveBacktestRunsCount()).isEqualTo(0);
            Assertions.assertThat(LazyDataUpdaterProperties.isLastActiveBoundedRun()).isTrue();
            Assertions.assertThat(LazyDataUpdaterProperties.runAsOfDate()).isEqualTo(UPDATE_LIMIT_TO);
            //a run entry with an open time range latches nothing: the fallback chain is untouched
            LazyDataUpdaterProperties.increaseActiveBacktestRunsCount(TimeRange.UNLIMITED);
            Assertions.assertThat(LazyDataUpdaterProperties.runAsOfDate()).isEqualTo(UPDATE_LIMIT_TO);
            LazyDataUpdaterProperties.decreaseActiveBacktestRunsCount();
            //nothing latched and no UPDATE_LIMIT_TO either: wall clock
            LazyDataUpdaterProperties.setUpdateLimitTo(FDates.MAX_DATE);
            Assertions.assertThat(LazyDataUpdaterProperties.runAsOfDate()).isAfter(UPDATE_LIMIT_TO);
        } finally {
            while (LazyDataUpdaterProperties.getActiveBacktestRunsCount() > 0) {
                LazyDataUpdaterProperties.decreaseActiveBacktestRunsCount();
            }
            LazyDataUpdaterProperties.setUpdateLimitTo(FDates.MAX_DATE);
        }
    }

}
