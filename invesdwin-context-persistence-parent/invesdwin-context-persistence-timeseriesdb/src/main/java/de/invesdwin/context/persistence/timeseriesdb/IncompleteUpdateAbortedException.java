package de.invesdwin.context.persistence.timeseriesdb;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.retry.IRetryDisabledException;

/**
 * The attempt's chain is internally inconsistent - so resuming from what it published is NOT safe, and re-asking will
 * reproduce the same contradiction rather than fix it.
 * <p>
 * This is the counterpart of {@link IncompleteUpdateRetryableException}, which says "this pass did not finish, the
 * published prefix is sound, continue above it". Until this class carried a producer, every failure of either kind went
 * through the same conversion at {@code ATimeSeriesUpdater}'s catch ({@link
 * IncompleteUpdateRetryableException#propagateIncompleteUpdateException(Throwable)}) and came out as "retry later": a
 * byte-space regression inside the index chain, or a summary whose own stamps disagree with the values at its ends, was
 * retried as if it were a supply problem. Measured consequence: cap35r on 2026-11-06, where
 * {@code prevMemoryFileSize[N] should be less than expectedMemoryFileFize[M]} - a store invariant, not a transient
 * condition - produced 1,257 re-asks of one demand across 51+ retries with cumulative waits past PT34M.
 * <p>
 * It extends {@link IllegalStateException} deliberately: every existing catcher of the chain-consistency refusals (the
 * high-water guard cell is one) keeps working unchanged - the new information is the TYPE, not a re-parenting of the
 * contract. And it implements {@link IRetryDisabledException} so the retry engine's chain walk refuses it at once:
 * the producer declares permanence, and no wrapper downstream has to guess.
 */
@NotThreadSafe
public class IncompleteUpdateAbortedException extends IllegalStateException implements IRetryDisabledException {

    public IncompleteUpdateAbortedException() {
        super();
    }

    public IncompleteUpdateAbortedException(final String message) {
        super(message);
    }

    public IncompleteUpdateAbortedException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public IncompleteUpdateAbortedException(final Throwable cause) {
        super(cause);
    }

}
