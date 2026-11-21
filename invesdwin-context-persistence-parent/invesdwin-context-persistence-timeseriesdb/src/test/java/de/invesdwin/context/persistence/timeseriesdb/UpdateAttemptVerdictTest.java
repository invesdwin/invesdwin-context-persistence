package de.invesdwin.context.persistence.timeseriesdb;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.retry.IRetryLaterException;
import de.invesdwin.context.integration.retry.RetryDisabledRuntimeException;
import de.invesdwin.context.integration.retry.RetryLaterPendingRuntimeException;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.error.Throwables;

/**
 * What an update attempt answers when its chain contains an incomplete update.
 * <p>
 * {@link IncompleteUpdateRetryableException} is the store's declaration that this pass did not finish and that the next
 * one may continue from the published prefix. It is not a licence to call every failure underneath it transient: the
 * chain walk in {@code ExceptionCauseRetryPolicy.shouldRetry} ends immediately at an
 * {@link de.invesdwin.context.integration.retry.IRetryDisabledException}, and a wrapper that hides such a verdict turns
 * a permanent refusal into an unbounded re-ask. On the consuming side of this exact wrapper that was measured: one
 * demand whose store failure arrived wrapped as incomplete re-asked 1,257 times over 51+ retries with cumulative waits
 * past PT34M (cap35r, 2026-11-06).
 */
@NotThreadSafe
public class UpdateAttemptVerdictTest {

    @Test
    public void testADeclaredPermanenceUnderTheMarkerIsNotTurnedIntoAWait() {
        final RetryDisabledRuntimeException permanent = new RetryDisabledRuntimeException(
                "the range can not be served by this source");
        final RuntimeException t = new RuntimeException(new IncompleteUpdateRetryableException(
                new IllegalStateException("read failed", permanent)));

        final Throwable thrown = verdictOf("hashKey[1]", t);

        Assertions.checkSame(t, thrown,
                "a declared permanence must propagate unchanged, so the retry engine reads the marker it can see: "
                        + thrown);
        Assertions.checkTrue(!Throwables.isCausedByType(thrown, IRetryLaterException.class),
                "the store's 'could be retried' must not re-label the producer's permanent verdict: " + thrown);
        Assertions.checkTrue(Throwables.isCausedByType(thrown, RetryDisabledRuntimeException.class),
                "the disabled marker must stay in the chain: " + thrown);
    }

    @Test
    public void testAProvenPendingWaitStaysPendingAtTheTopLayer() {
        final RuntimeException t = new RuntimeException(new IncompleteUpdateRetryableException(
                new RetryLaterPendingRuntimeException(
                        RetryLaterPendingRuntimeException.PENDING_PREFIX + "update lock held by a running updater")));

        final Throwable thrown = verdictOf("hashKey[2]", t);

        Assertions.checkTrue(RetryLaterPendingRuntimeException.isPending(thrown),
                "a pending wait must stay pending through the wrapper: " + thrown);
        Assertions.checkTrue(thrown instanceof RetryLaterPendingRuntimeException,
                "the TOP layer must be the pending type: the budget counts by walking the chain either way"
                        + " (LoggingPreviousCause:28,:74), so the type is what the reporting layer reads: " + thrown);
        Assertions.checkTrue(thrown.getStackTrace().length == 0,
                "a productive wait must not capture a stack per attempt, was " + Integer.valueOf(
                        thrown.getStackTrace().length) + " frames");
    }

    @Test
    public void testThePendingRecognizerAlsoWorksWithATagOnlyPending() {
        //this store's refusals cross a wire boundary where the subtype is lost and only the message text survives
        final RuntimeException t = new RuntimeException(new IncompleteUpdateRetryableException(
                new RetryLaterRuntimeException(RetryLaterPendingRuntimeException.PENDING_PREFIX
                        + "waiting for the crawler backlog to drain")));

        final Throwable thrown = verdictOf("hashKey[3]", t);

        Assertions.checkTrue(thrown instanceof RetryLaterPendingRuntimeException,
                "a tag-only pending must still be promoted to a pending verdict: " + thrown);
    }

    @Test
    public void testAGenuinelyIncompleteUpdateStaysRetryLaterAndNamesTheKey() {
        final RuntimeException t = new RuntimeException(new IncompleteUpdateRetryableException(
                "the source died mid-pass"));

        final Throwable thrown = verdictOf("hashKey[4]", t);

        Assertions.checkTrue(thrown instanceof RetryLaterRuntimeException,
                "an incomplete update with nothing else declared stays a wait, but was: " + thrown);
        Assertions.checkTrue(!RetryLaterPendingRuntimeException.isPending(thrown),
                "it must not be counted as pending - no work is in flight for this attempt: " + thrown);
        Assertions.checkTrue(String.valueOf(thrown.getMessage()).contains("hashKey[4]"),
                "the verdict names the key it refuses for, or the log line cannot be attributed: " + thrown);
    }

    private static Throwable verdictOf(final String key, final RuntimeException t) {
        try {
            throw UpdateAttemptVerdict.forIncompleteUpdate(key, t);
        } catch (final Throwable thrown) {
            return thrown;
        }
    }

}
