package de.invesdwin.context.persistence.timeseries.ipc;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;

// CHECKSTYLE:OFF
@NotThreadSafe
public class ASpinWaitTest extends ATest {
    //CHECKSTYLE:ON

    @Test
    public void testMaxWait() throws IOException {
        final ASpinWait waitingSpinWait = new ASpinWait() {

            @Override
            protected boolean isConditionFulfilled() {
                return false;
            }
        };
        final Duration maxWait = Duration.ONE_SECOND;
        final Instant waitingSince = new Instant();
        waitingSpinWait.awaitFulfill(waitingSince, maxWait);
        Assertions.assertThat(waitingSince.toDuration()).isGreaterThanOrEqualTo(maxWait);
    }

    @Test
    public void testNoWait() throws IOException {
        final ASpinWait waitingSpinWait = new ASpinWait() {

            @Override
            protected boolean isConditionFulfilled() {
                return true;
            }
        };
        final Duration maxWait = Duration.ONE_SECOND;
        final Instant waitingSince = new Instant();
        waitingSpinWait.awaitFulfill(waitingSince, maxWait);
        Assertions.assertThat(waitingSince.toDuration()).isLessThan(maxWait);
    }

}
