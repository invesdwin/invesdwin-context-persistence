package de.invesdwin.context.persistence.timeseriesdb.updater;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.time.duration.Duration;

/**
 * Publish-then-yield: the handle through which one update attempt gives up the key's table write lock at a boundary
 * commit and takes it back before continuing with the next batch. At a boundary commit the store's visible state is
 * exactly the published generations (the publish landed and the writer rotated off the file it published), so the
 * release keeps the single-writer invariant while the attempt does not hold the lock. The re-acquire is bounded by
 * {@link TimeSeriesProperties#UPDATE_YIELD_ACQUIRE_WRITE_LOCK_TIMEOUT}: a reader that still holds the read side past
 * that window ends the attempt cleanly instead of the writer convoying on its own key forever - the published prefix
 * is the resume point, so the clean end costs no progress. The holder tracking lets the attempt-end path release the
 * lock exactly once whether it is still held from the last batch or was already yielded and never taken back.
 */
@NotThreadSafe
final class TimeSeriesWriteLockYield {

    //after releasing the write lock, let the readers parked on it actually land in the read side before contending
    //for re-acquisition: on this non-fair read-write lock a writer that releases and immediately tryLocks usually
    //wins its own race, which would make the released window no window at all
    private static final long WINDOW_GRACE_MILLIS = 5;

    private final ILock writeLock;
    private final Duration reacquireTimeout;
    private boolean held;

    TimeSeriesWriteLockYield(final ILock writeLock, final Duration reacquireTimeout) {
        this.writeLock = writeLock;
        this.reacquireTimeout = reacquireTimeout;
        this.held = true;
    }

    boolean isHeld() {
        return held;
    }

    /**
     * Releases the table write lock at a published boundary and tries to take it back. True when re-acquired (the
     * attempt continues its next batch still single-writing), false when readers of the key kept it (the attempt must
     * end cleanly at the published boundary).
     */
    boolean yieldAndTryReacquire() {
        Assertions.checkTrue(held, "the update attempt must hold the table write lock [%s] to yield it",
                writeLock.getName());
        writeLock.unlock();
        held = false;
        try {
            Thread.sleep(WINDOW_GRACE_MILLIS);
            if (writeLock.tryLock(reacquireTimeout)) {
                held = true;
                return true;
            }
            return false;
        } catch (final InterruptedException e) {
            //an interrupted attempt cannot continue writing nor re-establish the single-writer invariant: end it like
            //a failed re-acquisition, the published boundaries stay
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Attempt-end path: releases the table write lock exactly once if this lease still holds it.
     */
    void releaseIfHeld() {
        if (held) {
            writeLock.unlock();
            held = false;
        }
    }

}
