package de.invesdwin.context.persistence.timeseriesdb.updater;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.time.date.FDate;

/**
 * The recovery policy for an update pass on these rails ({@link ATimeSeriesUpdater} /
 * {@link TimeSeriesUpdateTransaction}) that does not end in one clean run. The rails publish at segment boundaries
 * ({@link ITimeSeriesBoundaryCommitListener}), discard only the aborted attempt's UNPUBLISHED tail, and resume the next
 * attempt above the published prefix - so two caller-side decisions destroy what the source already answered and must
 * not be made by the callers any more:
 * <ul>
 * <li>on an {@code IncompleteUpdateRetryableException}, dropping the whole instrument range ({@code deleteRange}) and
 * clearing the crawl-progress key throws away every answered range this and all earlier attempts had materialised, and
 * the next attempt re-pays the whole crawl from the anchor;</li>
 * <li>a completed pass that consumed its requested range and stored nothing is a declared-empty answer (holiday-only or
 * out-of-life range, an expired or uncovered instrument, a tail that has not moved yet), not a program error; throwing
 * (typically on the null {@code getUpdatedTo()}) left the crawl-progress key unwritten, so the finished gate kept
 * re-asking the same dead range.</li>
 * </ul>
 * So: the published payload and the progress key stay whenever the store has a published frontier; only an instrument
 * that never published anything is rebuilt, and an answered-nothing completed pass is logged and recorded as answered up
 * to the download horizon instead of throwing. The empty answer is the table layer's equivalent of the file layer's
 * 21-byte empty interval file: the range was asked and answered empty, so it must be recorded as answered instead of
 * being re-asked forever.
 * <p>
 * Extracted from three byte-identical per-source copies (the iqfeed minute/day store, the interactive-brokers bar
 * store and the personal-webdav store, all on {@link ALazyDataUpdater}); the per-source call sites keep their own
 * wording and payload wiring, this class owns the decision.
 */
@Immutable
public final class TimeSeriesUpdateRecovery {

    /**
     * What to do with the payload an interrupted attempt left behind.
     */
    @Immutable
    public enum Recovery {
        /** The attempt published a prefix: keep it, keep the crawl-progress key, let the next attempt resume. */
        KEEP_PUBLISHED_PREFIX,
        /** Nothing was ever published for this instrument: the range may be rebuilt from scratch. */
        REBUILD_FROM_SCRATCH
    }

    private TimeSeriesUpdateRecovery() {
        //static policy
    }

    /**
     * The store's published frontier (latest stored value key) as the attempt ended it. A non-null frontier means the
     * store holds answered ranges that are readable right now, and the store's own transaction machinery resumes the
     * next attempt above it - so the payload must not be deleted.
     */
    public static Recovery onIncompleteUpdate(final FDate publishedFrontier) {
        return publishedFrontier != null ? Recovery.KEEP_PUBLISHED_PREFIX : Recovery.REBUILD_FROM_SCRATCH;
    }

    /**
     * Whether a completed pass that stored nothing must be escalated. It must not: the pass consumed its range, the
     * source answered no element in it, and that answer is only worth a warning with the range around it.
     */
    public static boolean isAnsweredNothingPass(final FDate updatedTo) {
        return updatedTo == null;
    }

    /**
     * What the interrupted attempt's owner can do to the payload and to the crawl-progress key; kept behind an
     * interface so the policy is checkable without a live store.
     */
    public interface IPublishedPayload {

        void deletePublishedPayload();

        void clearDownloadedLastDate();

    }

    /**
     * Applies the recovery decision of {@link #onIncompleteUpdate(FDate)}: only an instrument that never published
     * anything gets its range dropped. The caller then rides the ordinary retry rails, and the store's update
     * transaction resumes the next attempt above the published high-water.
     */
    public static Recovery recoverFromIncompleteUpdate(final IPublishedPayload publishedPayload,
            final FDate publishedFrontier) {
        final Recovery recovery = onIncompleteUpdate(publishedFrontier);
        if (recovery == Recovery.REBUILD_FROM_SCRATCH) {
            publishedPayload.deletePublishedPayload();
            publishedPayload.clearDownloadedLastDate();
        }
        return recovery;
    }

    /**
     * The crawl-progress key after a pass that ran to its end - whether or not a single element landed. The pass asked
     * the whole requested range, so the range is answered; leaving the key behind is what keeps the finished gate from
     * demanding the same crawl again on every attempt.
     */
    public static FDate newDownloadedLastDateAfterCompletedPass(final FDate maxDownloadDateExclusive) {
        return maxDownloadDateExclusive;
    }

}
