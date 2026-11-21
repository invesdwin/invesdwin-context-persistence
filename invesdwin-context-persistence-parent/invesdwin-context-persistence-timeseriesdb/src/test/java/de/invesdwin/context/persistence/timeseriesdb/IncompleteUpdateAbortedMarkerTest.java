package de.invesdwin.context.persistence.timeseriesdb;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.retry.Retries;
import de.invesdwin.context.integration.retry.RetryDisabledRuntimeException;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.error.Throwables;

/**
 * The aborted marker finally means something.
 * <p>
 * {@link IncompleteUpdateAbortedException} has existed since the store learned to distinguish "this pass did not
 * finish" from "this pass produced a chain that contradicts itself" - but nothing threw it, so the distinction the
 * conversion helper implements ({@link IncompleteUpdateRetryableException#propagateIncompleteUpdateException(Throwable)})
 * was unreachable, and every write-side invariant failure came out of {@code ATimeSeriesUpdater} as "resume later".
 * That is the mechanism behind cap35r (2026-11-06): a byte-space regression inside the index chain - a store invariant,
 * not a supply condition - was re-asked 1,257 times across 51+ retries with cumulative waits past PT34M.
 * <p>
 * These cells pin the three properties the marker has to keep once it has producers: the conversion refuses to relabel
 * it, the retry machinery stops at it, and its IllegalStateException supertype keeps every existing catcher of the
 * chain-consistency refusals working - the new information is the type, not a changed contract.
 */
@NotThreadSafe
public class IncompleteUpdateAbortedMarkerTest {

    @Test
    public void testTheConversionDoesNotRelabelAnAbortedChainAsResumeLater() {
        final IncompleteUpdateAbortedException aborted = new IncompleteUpdateAbortedException(
                "prevMemoryFileSize[4193405] should be less than expectedMemoryFileFize[4193368]");
        final RuntimeException attemptFailure = new RuntimeException(aborted);

        //the helper propagates the ATTEMPT failure (the wrapper), keeping the producer's marker in the chain - which is
        //what the retry engine walks, so the chain is what has to be right rather than the outer type
        Throwable escaped = null;
        try {
            IncompleteUpdateRetryableException.propagateIncompleteUpdateException(attemptFailure);
        } catch (final Throwable t) {
            escaped = t;
        }
        Assertions.checkNotNull(escaped, "an aborted chain has to reach the caller as a failure, not as a normal end");
        Assertions.checkTrue(Throwables.isCausedByType(escaped, IncompleteUpdateAbortedException.class),
                "the producer's marker must survive the conversion inside the chain: " + escaped);
        Assertions.checkTrue(!Throwables.isCausedByType(escaped, IncompleteUpdateRetryableException.class),
                "and it must NOT have been relabelled as 'this pass did not finish, resume above the prefix' - that"
                        + " re-label is what turned a store invariant into an unbounded re-ask: " + escaped);
    }

    /**
     * The case the marker is actually for. A chain-consistency failure rarely travels alone - it usually sits on top of
     * something the retry engine WOULD allow (an {@code IOException} is in its allowed-cause list, and the store's own
     * reads raise it constantly). Without a declared marker, the allowed cause wins and the demand re-asks a chain that
     * can never become consistent; with it, the walk stops at the permanence. Written after the first version of this
     * cell turned out to be vacuous: it asserted a no-retry outcome that the engine also produces for an undeclared
     * failure, so dropping the marker interface left it green.
     */
    @Test
    public void testTheMarkerOutweighsAnOtherwiseAllowedCauseUnderneath() {
        final IOException allowedUnderlying = new IOException("Stream is corrupted");

        final RuntimeException declared = new RuntimeException(
                new IncompleteUpdateAbortedException("summary.firstValue.endTime disagrees with the summary stamp",
                        allowedUnderlying));
        Assertions.checkFalse(Retries.shouldRetry(declared),
                "the chain walk must end at the disabled marker even though an allowed cause sits under it");
        Assertions.checkTrue(Retries.toRetryVerdict(declared) instanceof RetryDisabledRuntimeException,
                "and the consumer-visible verdict must be permanence, not a wait: "
                        + Retries.toRetryVerdict(declared));

        //the contrast: the SAME failure shape without the marker is retried, because IOException is an allowed cause
        final RuntimeException undeclared = new RuntimeException(
                new IllegalStateException("summary.firstValue.endTime disagrees with the summary stamp",
                        allowedUnderlying));
        Assertions.checkTrue(Retries.shouldRetry(undeclared),
                "precondition for the contrast: without the marker the allowed-cause list decides, which is exactly"
                        + " the loop the marker exists to break");
        Assertions.checkTrue(Retries.toRetryVerdict(undeclared) instanceof RetryLaterRuntimeException,
                "so without the marker the consumer is handed a wait: " + Retries.toRetryVerdict(undeclared));
    }

    @Test
    public void testTheMarkerKeepsTheIllegalStateContractOfTheChainGuards() {
        final IncompleteUpdateAbortedException aborted = new IncompleteUpdateAbortedException("guard refusal");
        Assertions.checkTrue(aborted instanceof IllegalStateException,
                "every existing catcher of the chain-consistency refusals catches IllegalStateException - the marker"
                        + " may not re-parent the contract it strengthens");
        try {
            throw aborted;
        } catch (final IllegalStateException e) {
            Assertions.checkTrue(e.getMessage().contains("guard refusal"),
                    "and the refusal still reads the same to a catcher that does not know about the marker");
        }
    }

}
