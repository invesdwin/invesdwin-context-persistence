package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.Locks;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.date.millis.FDateMillis;
import de.invesdwin.util.time.duration.Duration;

/**
 * The willing-to-wait tier's join onto another context's update round is bounded, not permanent.
 * <p>
 * The code base had 56 unbounded {@code .lock()} acquisitions against 9 zero-wait {@code tryLock()} on the store
 * trunk; the one here was on the update lock, past a bounded first attempt twelve lines above. A production hang made
 * the cost concrete: two threads parked 9 h 27 min in that shape, with no monitor cycle for jstack to report, and the
 * crawler's initial-download gate stayed shut for every other instrument of the family.
 */
@NotThreadSafe
public class StoreUpdateJoinCeilingTest {

    @Test
    public void testTheJoinCeilingShippedIsAFiniteWatchdog() {
        final Duration ceiling = TimeSeriesProperties.UPDATE_JOIN_ACQUIRE_TIMEOUT;
        Assertions.checkNotNull(ceiling, "the join tier must have a ceiling at all");
        final long millis = ceiling.longValue(FTimeUnit.MILLISECONDS);
        Assertions.checkTrue(millis > 0L, "the ceiling must be a real wait, not a zero-wait refusal of a join:"
                + Long.valueOf(millis));
        Assertions.checkTrue(millis <= 30L * 60L * 1000L,
                "the ceiling must stay a watchdog rather than an effectively-permanent park: "
                        + Long.valueOf(millis) + " ms");
    }

    @Test
    public void testTheJoinAbsorbsAnOrdinaryHandOver() throws InterruptedException {
        final ILock updateLock = newLock("join-absorbs");
        final Holder holder = new Holder(updateLock, 250);
        holder.start();
        try {
            Assertions.checkTrue(ALazyDataUpdater.acquireUpdateLockForJoin(updateLock,
                    new Duration(5, FTimeUnit.SECONDS)), "a join that would have waited before still gets the lock -"
                            + " the ceiling must not turn an ordinary hand-over into a refusal");
            updateLock.unlock();
        } finally {
            holder.finish();
        }
    }

    @Test
    public void testTheJoinIsRefusedRatherThanParkedPastItsCeiling() throws InterruptedException {
        final ILock updateLock = newLock("join-refused");
        final Holder holder = new Holder(updateLock, 500);
        holder.start();
        try {
            final long startMillis = FDateMillis.nowMillis();
            Assertions.checkTrue(!ALazyDataUpdater.acquireUpdateLockForJoin(updateLock,
                    new Duration(100, FTimeUnit.MILLISECONDS)), "past the ceiling the join must be refused");
            final long elapsedMillis = FDateMillis.nowMillis() - startMillis;
            Assertions.checkTrue(elapsedMillis >= 100L, "the refusal must come at the ceiling: "
                    + Long.valueOf(elapsedMillis) + " ms");
            Assertions.checkTrue(elapsedMillis < 450L, "and not after the holder's round - the old code parked until"
                    + " the holder finished, which is how a hung update hung the caller forever");
            Assertions.checkTrue(!updateLock.isHeldByCurrentThread(), "a refused join holds nothing");
        } finally {
            holder.finish();
        }
    }

    private static ILock newLock(final String name) {
        return Locks.newReentrantLock("StoreUpdateJoinCeilingTest_" + name);
    }

    /** A real second thread: an in-process acquire of a reentrant lock would succeed and prove nothing. */
    private static final class Holder {

        private final ILock lock;
        private final int holdMillis;
        private final Thread thread;
        private final AtomicBoolean held = new AtomicBoolean();

        private Holder(final ILock lock, final int holdMillis) {
            this.lock = lock;
            this.holdMillis = holdMillis;
            this.thread = new Thread(() -> {
                lock.lock();
                try {
                    held.set(true);
                    Thread.sleep(holdMillis);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    lock.unlock();
                }
            }, "StoreUpdateJoinCeilingTest-holder");
        }

        private void start() throws InterruptedException {
            thread.start();
            final long deadline = FDateMillis.nowMillis() + 5000L;
            while (!held.get() && FDateMillis.nowMillis() < deadline) {
                Thread.sleep(5L);
            }
            Assertions.checkTrue(held.get(), "precondition: the holder must own the update lock");
        }

        private void finish() throws InterruptedException {
            thread.join(20000L);
        }
    }
}
