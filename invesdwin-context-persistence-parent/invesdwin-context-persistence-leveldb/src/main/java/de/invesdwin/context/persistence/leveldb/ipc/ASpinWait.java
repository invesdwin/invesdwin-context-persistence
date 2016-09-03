package de.invesdwin.context.persistence.leveldb.ipc;

import java.io.IOException;
import java.util.concurrent.locks.LockSupport;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FTimeUnit;

@Immutable
public abstract class ASpinWait {

    protected final boolean spinAllowed = determineSpinAllowed();
    /**
     * The number of times to spin before blocking in timed waits. The value is empirically derived -- it works well
     * across a variety of processors and OSes. Empirically, the best value seems not to vary with number of CPUs
     * (beyond 2) so is just a constant.
     */
    protected final int maxTimedSpins = (spinAllowed) ? determineMaxTimedSpins() : 0;

    /**
     * The number of times to spin before blocking in untimed waits. This is greater than timed value because untimed
     * waits spin faster since they don't need to check times on each spin.
     */
    protected final int maxUntimedSpins = determineMaxUntimedSpins();

    /**
     * The number of nanoseconds for which it is faster to spin rather than to use timed park. A rough estimate
     * suffices.
     */
    protected final long skipSpinAfterWaitingSince = determineSkipSpinAfterWaitingSince()
            .longValue(FTimeUnit.NANOSECONDS);
    /**
     * How long each park interval should last before we check again if the condition is fulfilled.
     */
    protected final long maxParkIntervalNanos = determineMaxParkInterval().longValue(FTimeUnit.NANOSECONDS);
    /**
     * How long we want to
     */
    protected final long maxTimedSpinDuration = determineMaxTimedSpinDuration().longValue(FTimeUnit.NANOSECONDS);

    protected Duration determineSkipSpinAfterWaitingSince() {
        //when we have been waiting a long time for a request/response we should keep the CPU usage to a minimum and thus don't even try to spin
        return new Duration(1, FTimeUnit.SECONDS);
    }

    protected int determineMaxUntimedSpins() {
        return maxTimedSpins * 16;
    }

    /**
     * We need a magnitude more than SynchronousQueue since two processes are involved
     */
    protected int determineMaxTimedSpins() {
        return 32 * 1000;
    }

    protected boolean determineSpinAllowed() {
        return Runtime.getRuntime().availableProcessors() >= 2;
    }

    /**
     * with 1 microsecond sleep, the performance penalty is not too large while still keeping the CPU usage at minimum
     */
    protected Duration determineMaxParkInterval() {
        return new Duration(1, FTimeUnit.MICROSECONDS);
    }

    /**
     * since we have IPC the 1000 nanoseconds from SynchronousQueue for spinning are too short to optimal performance
     */
    public Duration determineMaxTimedSpinDuration() {
        return new Duration(10, FTimeUnit.MICROSECONDS);
    }

    protected abstract boolean isConditionFulfilled() throws IOException;

    protected boolean isSpinAllowed(final Instant waitingSince) {
        return waitingSince.toDuration().longValue(FTimeUnit.NANOSECONDS) < skipSpinAfterWaitingSince;
    }

    public boolean awaitFulfill(final Instant waitingSince) throws IOException {
        while (true) {
            awaitFulfill(waitingSince, Duration.ONE_DAY);
        }
    }

    public boolean awaitFulfill(final Instant waitingSince, final Duration maxWait) throws IOException {
        if (isConditionFulfilled()) {
            return true;
        }
        final boolean spinAllowedNow = spinAllowed && isSpinAllowed(waitingSince);
        if (spinAllowedNow) {
            for (int untimedSpins = 0; untimedSpins < maxUntimedSpins; untimedSpins++) {
                if (isConditionFulfilled()) {
                    return true;
                }
            }
        }
        long nanosRemaining = maxWait.longValue(FTimeUnit.NANOSECONDS);
        final long waitDeadline = System.nanoTime() + nanosRemaining;
        final Thread w = Thread.currentThread();
        int timedSpins = 0;
        while (true) {
            if (isConditionFulfilled()) {
                return true;
            }
            nanosRemaining = waitDeadline - System.nanoTime();
            if (nanosRemaining <= 0L) {
                //we have exceeded maxWait
                return false;
            }
            final boolean shouldSpin = spinAllowedNow && nanosRemaining < maxTimedSpinDuration
                    && timedSpins < maxTimedSpins;
            if (shouldSpin) {
                timedSpins++;
            } else {
                //only check interrupted when we are on the slow lane anyway
                if (w.isInterrupted()) {
                    return false;
                }
                LockSupport.parkNanos(this, maxParkIntervalNanos);
            }
        }
    }

}
