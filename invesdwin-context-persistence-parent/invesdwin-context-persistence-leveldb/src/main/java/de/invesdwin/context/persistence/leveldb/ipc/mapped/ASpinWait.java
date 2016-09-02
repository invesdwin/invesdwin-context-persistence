package de.invesdwin.context.persistence.leveldb.ipc.mapped;

import java.util.concurrent.locks.LockSupport;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FTimeUnit;

@ThreadSafe
public abstract class ASpinWait {

    private static final boolean SPIN_ALLOWED = Runtime.getRuntime().availableProcessors() >= 2;

    /**
     * The number of nanoseconds for which it is faster to spin rather than to use timed park. A rough estimate
     * suffices.
     */

    protected final long skipSpinAfterWaitingSince = getSkipSpinAfterWaitingSince().longValue(FTimeUnit.NANOSECONDS);
    protected final long maxParkIntervalNanos = getMaxParkInterval().longValue(FTimeUnit.NANOSECONDS);
    protected final long maxSpinDuration = getMaxSpinDuration().longValue(FTimeUnit.NANOSECONDS);

    protected Duration getSkipSpinAfterWaitingSince() {
        //when we have been waiting a long time for a request/response we should keep the CPU usage to a minimum and thus don't even try to spin
        return new Duration(1, FTimeUnit.SECONDS);
    }

    /**
     * with 1 microsecond sleep, the performance penalty is not too large while still keeping the CPU usage at minimum
     */
    protected Duration getMaxParkInterval() {
        return new Duration(1, FTimeUnit.MICROSECONDS);
    }

    /**
     * since we have IPC the 1000 nanoseconds from SynchronousQueue for spinning are too short to optimal performance
     */
    public Duration getMaxSpinDuration() {
        return new Duration(10, FTimeUnit.MICROSECONDS);
    }

    protected abstract boolean isConditionFulfilled();

    protected boolean shouldSpin(final Instant waitingSince) {
        return waitingSince.toDuration().longValue(FTimeUnit.NANOSECONDS) < skipSpinAfterWaitingSince;
    }

    public boolean awaitFulfill(final Instant waitingSince) {
        while (true) {
            awaitFulfill(waitingSince, new Duration(1, FTimeUnit.DAYS));
        }
    }

    public boolean awaitFulfill(final Instant waitingSince, final Duration maxWait) {
        long nanosRemaining = maxWait.longValue(FTimeUnit.NANOSECONDS);
        final long waitDeadline = System.nanoTime() + nanosRemaining;
        final Thread w = Thread.currentThread();
        final boolean spinAllowed = SPIN_ALLOWED && shouldSpin(waitingSince);
        for (@SuppressWarnings("unused")
        int i = 0;; i++) {
            if (w.isInterrupted()) {
                return false;
            }
            if (isConditionFulfilled()) {
                return true;
            }
            nanosRemaining = waitDeadline - System.nanoTime();
            if (nanosRemaining <= 0L) {
                //we have exceeded maxWait
                return false;
            }
            if (spinAllowed && nanosRemaining < maxSpinDuration) {
                continue;
            } else {
                LockSupport.parkNanos(this, Math.min(nanosRemaining, maxParkIntervalNanos));
            }
        }
    }

}
