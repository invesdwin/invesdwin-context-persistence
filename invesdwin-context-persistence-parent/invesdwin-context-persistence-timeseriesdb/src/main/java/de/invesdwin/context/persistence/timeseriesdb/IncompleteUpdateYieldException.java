package de.invesdwin.context.persistence.timeseriesdb;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Publish-then-yield: a boundary commit released the update attempt's table write lock so readers of the key get a
 * window, and the attempt could not re-acquire it within
 * {@link TimeSeriesProperties#UPDATE_YIELD_ACQUIRE_WRITE_LOCK_TIMEOUT}. This ends the attempt CLEANLY, not as a
 * failure: the boundary commit published what the attempt staged up to that segment, and that published prefix is the
 * resume point of the next attempt of the key, so a clean end loses no progress. {@link
 * de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater} turns it into an
 * {@link IncompleteUpdateRetryableException} for its callers, the same rails an aborted (source-death) attempt
 * already rides.
 */
@NotThreadSafe
public class IncompleteUpdateYieldException extends RuntimeException {

    public IncompleteUpdateYieldException() {
        super();
    }

    public IncompleteUpdateYieldException(final String message) {
        super(message);
    }

    public IncompleteUpdateYieldException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public IncompleteUpdateYieldException(final Throwable cause) {
        super(cause);
    }

}
