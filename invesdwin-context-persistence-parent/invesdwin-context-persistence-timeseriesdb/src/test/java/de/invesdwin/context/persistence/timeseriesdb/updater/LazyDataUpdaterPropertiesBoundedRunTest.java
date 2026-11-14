package de.invesdwin.context.persistence.timeseriesdb.updater;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FDates;

/**
 * The run-count lifecycle of {@link LazyDataUpdaterProperties}: a run entry that finds no UPDATE_LIMIT_TO pins one for
 * the duration of the run, an UPDATE_LIMIT_TO that was set from outside is left alone, and the last closer restores
 * "nothing pinned" together with clearing the per-updater markers, so the next run can update fully again.
 */
@NotThreadSafe
public class LazyDataUpdaterPropertiesBoundedRunTest {

    private static final FDate EXTERNAL_UPDATE_LIMIT_TO = FDateBuilder.newDate(2000, 1, 1);
    private static final String UPDATER_ID = "LazyDataUpdaterPropertiesBoundedRunTest.updater";

    @Test
    public void testExternallyPinnedUpdateLimitToIsLeftAlone() {
        try {
            drainRuns();
            LazyDataUpdaterProperties.setUpdateLimitTo(EXTERNAL_UPDATE_LIMIT_TO);
            LazyDataUpdaterProperties.increaseActiveBacktestRunsCount();
            Assertions.assertThat(LazyDataUpdaterProperties.getActiveBacktestRunsCount()).isEqualTo(1);
            Assertions.assertThat(LazyDataUpdaterProperties.getUpdateLimitTo()).isEqualTo(EXTERNAL_UPDATE_LIMIT_TO);
            //the entry splits into 3 tasks: the count follows tasks, not entries
            LazyDataUpdaterProperties.increaseActiveBacktestRunsCount(3);
            Assertions.assertThat(LazyDataUpdaterProperties.getActiveBacktestRunsCount()).isEqualTo(4);
            //an updater that already ran books a marker, and that marker caps its later demands in the same JVM
            LazyDataUpdaterProperties.setLastUpdateTo(UPDATER_ID, EXTERNAL_UPDATE_LIMIT_TO);
            Assertions.assertThat(LazyDataUpdaterProperties.getUpdateLimitTo(UPDATER_ID))
                    .isEqualTo(EXTERNAL_UPDATE_LIMIT_TO);
            LazyDataUpdaterProperties.decreaseActiveBacktestRunsCount(3);
            LazyDataUpdaterProperties.decreaseActiveBacktestRunsCount();
            Assertions.assertThat(LazyDataUpdaterProperties.getActiveBacktestRunsCount()).isEqualTo(0);
            //not a run-pinned limit-to, so the last closer restores nothing and leaves the marker booked
            Assertions.assertThat(LazyDataUpdaterProperties.getUpdateLimitTo()).isEqualTo(EXTERNAL_UPDATE_LIMIT_TO);
            Assertions.assertThat(LazyDataUpdaterProperties.getLastUpdateTo(UPDATER_ID))
                    .isEqualTo(EXTERNAL_UPDATE_LIMIT_TO);
        } finally {
            drainRuns();
            LazyDataUpdaterProperties.setUpdateLimitTo(FDates.MAX_DATE);
        }
    }

    @Test
    public void testRunPinnedUpdateLimitToIsClearedByTheLastCloser() {
        try {
            drainRuns();
            LazyDataUpdaterProperties.setUpdateLimitTo(FDates.MAX_DATE);
            Assertions.assertThat(LazyDataUpdaterProperties.getUpdateLimitTo()).isNull();
            //the entry finds nothing set, so it pins one (the wall clock date) for the whole run
            LazyDataUpdaterProperties.increaseActiveBacktestRunsCount(2);
            final FDate pinnedLimitTo = LazyDataUpdaterProperties.getUpdateLimitTo();
            Assertions.assertThat(pinnedLimitTo).isNotNull();
            //a marker booked while the run is active caps this updater's later demands
            LazyDataUpdaterProperties.setLastUpdateTo(UPDATER_ID, pinnedLimitTo);
            Assertions.assertThat(LazyDataUpdaterProperties.getUpdateLimitTo(UPDATER_ID)).isEqualTo(pinnedLimitTo);
            //a second entry still open keeps the pin alive; only the last closer drops it
            LazyDataUpdaterProperties.increaseActiveBacktestRunsCount();
            LazyDataUpdaterProperties.decreaseActiveBacktestRunsCount();
            Assertions.assertThat(LazyDataUpdaterProperties.getUpdateLimitTo()).isEqualTo(pinnedLimitTo);
            LazyDataUpdaterProperties.decreaseActiveBacktestRunsCount(2);
            Assertions.assertThat(LazyDataUpdaterProperties.getActiveBacktestRunsCount()).isEqualTo(0);
            Assertions.assertThat(LazyDataUpdaterProperties.getUpdateLimitTo()).isNull();
            Assertions.assertThat(LazyDataUpdaterProperties.getLastUpdateTo(UPDATER_ID)).isNull();
        } finally {
            drainRuns();
            LazyDataUpdaterProperties.setUpdateLimitTo(FDates.MAX_DATE);
        }
    }

    private static void drainRuns() {
        while (LazyDataUpdaterProperties.getActiveBacktestRunsCount() > 0) {
            LazyDataUpdaterProperties.decreaseActiveBacktestRunsCount();
        }
    }

}
