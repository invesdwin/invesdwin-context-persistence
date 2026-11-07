package de.invesdwin.context.persistence.timeseriesdb.updater;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;

/**
 * Pins the update-recovery semantics of {@link TimeSeriesUpdateRecovery} at their cp home: an interrupted attempt keeps
 * its published prefix (the rails publish at segment boundaries and resume above it, so the answered ranges are not the
 * caller's to destroy), only an instrument that never published anything is rebuilt, and an answered-nothing completed
 * pass is recorded as answered up to the download horizon instead of escalated. The fixture is shaped like what a real
 * transport answers: a full session day, a holiday day that answers as an empty frame, and a partial final day cut off
 * by the download horizon. Each of the three migrated sources keeps its own mirror of these pins next to its call site;
 * this is the policy's own test. Relocation of the byte-identical per-source policies, so these pins were green before
 * this class existed - the red-first step happened where the policy was introduced per source, not here.
 */
@NotThreadSafe
public class TimeSeriesUpdateRecoveryTest {

    /** end of the last bar of a full trading day. */
    private static final FDate FULL_DAY_LAST_BAR = FDateBuilder.newDate(2026, 11, 3, 21, 59, 59).addMilliseconds(999);
    /** the partial final day: the crawl is cut at the download horizon, mid-session. */
    private static final FDate PARTIAL_DAY_LAST_BAR = FDateBuilder.newDate(2026, 11, 5, 10, 16, 59).addMilliseconds(999);
    private static final FDate MAX_DOWNLOAD_DAY_EXCLUSIVE = FDateBuilder.newDate(2026, 11, 6);

    /**
     * A pass that answered three day frames (full day, holiday empty frame, partial final day) and published them at a
     * boundary commit must survive an interruption: the answered ranges are the payload the store exists for, and the
     * next attempt resumes above the published high-water instead of re-crawling from the anchor.
     */
    @Test
    public void interruptedAttemptKeepsTheAnsweredRanges() {
        final FixturePayload payload = new FixturePayload();
        final FDate publishedFrontier = publishedFrontierOfDayFrames(new DayFrame(FULL_DAY_LAST_BAR),
                new DayFrame(null),
                new DayFrame(PARTIAL_DAY_LAST_BAR));
        Assertions.assertThat(publishedFrontier).as("partial final day is the published high-water").isNotNull();

        final TimeSeriesUpdateRecovery.Recovery recovery = TimeSeriesUpdateRecovery.recoverFromIncompleteUpdate(
                payload, publishedFrontier);

        Assertions.assertThat(recovery).isEqualTo(TimeSeriesUpdateRecovery.Recovery.KEEP_PUBLISHED_PREFIX);
        Assertions.assertThat(payload.deletes).as("answered ranges must not be dropped").isZero();
        Assertions.assertThat(payload.clearedProgressKeys).as("crawl progress stays where the segments are")
                .isZero();
    }

    /**
     * An instrument whose every answered frame was empty (holiday-only or out-of-life window) has nothing published;
     * there is no payload to protect, so a rebuild is legitimate - and it is the only case that may drop the range.
     */
    @Test
    public void onlyAnInstrumentThatNeverPublishedAnythingIsRebuilt() {
        final FixturePayload payload = new FixturePayload();
        final FDate publishedFrontier = publishedFrontierOfDayFrames(new DayFrame(null),
                new DayFrame(null),
                new DayFrame(null));
        Assertions.assertThat(publishedFrontier).as("all frames answered empty").isNull();

        final TimeSeriesUpdateRecovery.Recovery recovery = TimeSeriesUpdateRecovery.recoverFromIncompleteUpdate(
                payload, publishedFrontier);

        Assertions.assertThat(recovery).isEqualTo(TimeSeriesUpdateRecovery.Recovery.REBUILD_FROM_SCRATCH);
        Assertions.assertThat(payload.deletes).isEqualTo(1);
        Assertions.assertThat(payload.clearedProgressKeys).isEqualTo(1);
    }

    /**
     * A pass that consumed its whole requested range and stored nothing is an answered-empty range, the table-layer
     * counterpart of the file layer's 21-byte empty interval file: it is recorded as answered up to the download
     * horizon and must not be escalated into a failure, because that is what makes the finished gate re-ask the dead
     * range on every later attempt.
     */
    @Test
    public void answeredNothingPassIsDeclaredAndNotEscalated() {
        Assertions.assertThat(TimeSeriesUpdateRecovery.isAnsweredNothingPass(null)).isTrue();
        Assertions.assertThat(TimeSeriesUpdateRecovery.newDownloadedLastDateAfterCompletedPass(
                MAX_DOWNLOAD_DAY_EXCLUSIVE)).as("the answered range is recorded up to the download horizon")
                        .isEqualTo(MAX_DOWNLOAD_DAY_EXCLUSIVE);
    }

    /**
     * The normal case stays unremarkable: a pass that stored elements reports its frontier and marks the range answered.
     */
    @Test
    public void passThatStoredElementsIsNotAnsweredNothing() {
        Assertions.assertThat(TimeSeriesUpdateRecovery.isAnsweredNothingPass(PARTIAL_DAY_LAST_BAR)).isFalse();
        Assertions.assertThat(TimeSeriesUpdateRecovery.newDownloadedLastDateAfterCompletedPass(
                MAX_DOWNLOAD_DAY_EXCLUSIVE)).isEqualTo(MAX_DOWNLOAD_DAY_EXCLUSIVE);
    }

    /**
     * The high-water a pass leaves behind: the last element end time the store can read, holiday frames contributing
     * nothing (they are answered, they just hold no element).
     */
    private static FDate publishedFrontierOfDayFrames(final DayFrame... frames) {
        FDate frontier = null;
        for (final DayFrame frame : frames) {
            final FDate lastElementEndTime = frame.getLastElementEndTime();
            if (lastElementEndTime != null && (frontier == null || lastElementEndTime.isAfterNotNullSafe(frontier))) {
                frontier = lastElementEndTime;
            }
        }
        return frontier;
    }

    /** One answered day as the transport answers it: session elements, or nothing (holiday / out of life). */
    private static final class DayFrame {
        private final FDate lastElementEndTime;

        private DayFrame(final FDate lastElementEndTime) {
            this.lastElementEndTime = lastElementEndTime;
        }

        private FDate getLastElementEndTime() {
            return lastElementEndTime;
        }
    }

    private static final class FixturePayload implements TimeSeriesUpdateRecovery.IPublishedPayload {
        private int deletes;
        private int clearedProgressKeys;

        @Override
        public void deletePublishedPayload() {
            deletes++;
        }

        @Override
        public void clearDownloadedLastDate() {
            clearedProgressKeys++;
        }
    }

}
