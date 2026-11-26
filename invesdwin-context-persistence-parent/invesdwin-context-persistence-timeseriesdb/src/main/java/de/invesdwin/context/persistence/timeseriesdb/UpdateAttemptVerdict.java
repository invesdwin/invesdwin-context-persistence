package de.invesdwin.context.persistence.timeseriesdb;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.retry.IRetryDisabledException;
import de.invesdwin.context.integration.retry.RetryLaterPendingRuntimeException;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.integration.retry.fast.FastRetryLaterPendingRuntimeException;
import de.invesdwin.util.error.Throwables;

/**
 * Verdict for an update attempt whose failure chain contains
 * {@link IncompleteUpdateRetryableException} - the store's own declaration that this pass did not finish and that the
 * next one may continue from the prefix it published.
 * <p>
 * What the marker does NOT say is anything about the failure underneath it. Callers used to answer the shape with a
 * flat {@code new RetryLaterRuntimeException(t)}, which overrules whatever the producer declared. The rule as shipped
 * in the retry module is the other way round ({@code Retries#toRetryVerdict(Throwable)}: "a producer's declared marker
 * is never re-labelled"), and {@code ExceptionCauseRetryPolicy.shouldRetry} gives an {@link IRetryDisabledException}
 * anywhere in the chain absolute precedence - it ends the walk immediately, and the default for an undeclared failure
 * is no retry at all. The shape was measured on the consuming side of this same wrapper: a store invariant failure
 * ("prevMemoryFileSize[N] should be less than expectedMemoryFileFize[M]") arriving as an incomplete update turned one
 * demand into 1,257 re-asks with retries past 51 and cumulative waits over PT34M (cap35r, 2026-11-06) until a budget
 * hook bounded the loop - which bounds the loop without ever classifying the failure.
 * <p>
 * The pending case is promoted to a PENDING verdict at the top layer for the reporting shape, not for the counting:
 * the budget's pending test walks the chain too ({@code LoggingPreviousCause:28,:74} compute
 * {@code pendingAttempts} with {@code RetryLaterPendingRuntimeException.isPending(previousCause)}), so a wrapper would
 * not change the count either way. The top-layer type is what {@code LoggingRetryHook.isProductiveWaitSkip} reads -
 * one skip line instead of an error record with a captured stack.
 * <p>
 * This class decides only the verdict. The destructive question - whether a range may be discarded because an attempt
 * failed - is the caller's, and the answer there is in {@code ASegmentedTimeSeriesLookupStorageCache} next to the call.
 */
@Immutable
public final class UpdateAttemptVerdict {

    private UpdateAttemptVerdict() {}

    /**
     * @param key what the attempt was loading, so a refusal can be attributed in the log.
     * @return the verdict to throw; a declared permanence is never returned as a wait - it is propagated as it is.
     */
    public static RuntimeException forIncompleteUpdate(final String key, final Throwable t) {
        if (Throwables.getCauseByType(t, IRetryDisabledException.class) != null) {
            //propagated unchanged, never wrapped: the chain already carries the permanence, and a retry-later wrapper
            //over it is what turns a permanent refusal into an unbounded re-ask
            return Throwables.propagate(t);
        } else if (RetryLaterPendingRuntimeException.isPending(t)) {
            return new FastRetryLaterPendingRuntimeException(RetryLaterPendingRuntimeException.PENDING_PREFIX
                    + "update of [" + key + "] is incomplete while the work that would finish it is proven in flight"
                    + " (pending marker in the failure chain) - the next attempt continues from the published prefix", t);
        } else {
            return new RetryLaterRuntimeException("update of [" + key + "] is incomplete: the attempt publishes every"
                    + " segment it finished, so the next one resumes from that prefix instead of rebuilding the range",
                    t);
        }
    }

}
