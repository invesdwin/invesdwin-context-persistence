package de.invesdwin.context.persistence.timeseriesdb.updater;

import de.invesdwin.util.time.date.FDate;

/**
 * Notified at every PUBLISHED segment boundary commit of an update attempt (the resumable rails of 5974d85c and the
 * publish-then-yield of cca6cf29): at that moment the store's visible state is exactly the published generations, and
 * the callback carries the new published frontier - the end time of the last published value. Consumers above the
 * store that mirror the store's frontier (the trading caching services' downloaded first/last date keys, which
 * their own reload predicates read back as the next download window) attach one through
 * {@link ATimeSeriesUpdater#newBoundaryCommitListener()} so their view
 * advances with each published segment instead of only after a complete update: an attempt that dies or yields
 * mid-pass still leaves its frontier visible one layer up, and the next attempt resumes at the published high-water
 * instead of rebasing over it.
 *
 * The listener is called synchronously on the attempt thread inside the boundary commit, while the attempt still
 * holds the key's table write lock (before the yield releases it), so the notified frontier cannot be overtaken by
 * another writer mid-callback. Not every completed segment boundary publishes: a boundary that has not accumulated
 * {@link TimeSeriesUpdateTransaction#PUBLISH_SEGMENTS_PER_GENERATION} segments since the last publish stages without
 * publishing and notifies nothing. What IS notified
 * is every publish, including the one {@link TimeSeriesUpdateTransaction#close()} does with whatever the attempt staged
 * after its last publish, committed or aborted; an attempt that staged nothing publishes nothing and notifies nothing.
 */
public interface ITimeSeriesBoundaryCommitListener {

    /**
     * The boundary commit published; {@code publishedEndTime} is the end time of the last value the published
     * generation now reads as the store's frontier.
     */
    void onBoundaryCommitPublished(FDate publishedEndTime);

}
