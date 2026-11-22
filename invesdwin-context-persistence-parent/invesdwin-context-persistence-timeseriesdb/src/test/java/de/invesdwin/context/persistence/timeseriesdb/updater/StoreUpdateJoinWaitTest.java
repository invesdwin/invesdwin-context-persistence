package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.concurrent.FinancialdataThreads;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.Locks;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.date.millis.FDateMillis;

/**
 * The join arm of {@code ALazyDataUpdater.maybeUpdateWaiting} parks: a caller that came to wait for another context's
 * update round has no other answer for its demand, so the acquire carries no ceiling and produces no verdict.
 * <p>
 * What bounds a wedged holder instead is progress, not the wall clock: an attempt keeps publishing content or the
 * store's heartbeat/lease machinery and the derived-store stall guard stop crediting a stalled key, and the cycle edges
 * that made the old 9 h 27 min production park possible are refused up front by {@code skipCycleForCaller} and
 * {@code refuseDeadlockingJoin}. The neighbouring demand arm - a caller that CAN answer "later" - keeps its bounded
 * acquire and its retry verdict; that split is pinned here too.
 */
@NotThreadSafe
public class StoreUpdateJoinWaitTest {

    @Test
    public void testTheJoinAbsorbsAnOrdinaryHandOver() throws InterruptedException {
        final ILock updateLock = newLock("join-absorbs");
        final Holder holder = new Holder(updateLock, 250);
        holder.start();
        try {
            ALazyDataUpdater.acquireUpdateLockForJoin(updateLock);
            Assertions.checkTrue(updateLock.isHeldByCurrentThread(),
                    "an ordinary hand-over must end with the joiner owning the lock, not with a verdict");
            updateLock.unlock();
        } finally {
            holder.finish();
        }
    }

    @Test
    public void testTheJoinParksThroughAHoldNoCeilingWouldSurvive() throws InterruptedException {
        //the case the old five-minute join ceiling existed for, now waited out: a hold longer than any small ceiling,
        //and the joiner still ends up with the lock rather than with a verdict it cannot use
        final ILock updateLock = newLock("join-parks");
        final Holder holder = new Holder(updateLock, 1_800);
        holder.start();
        try {
            final long startMillis = FDateMillis.nowMillis();
            ALazyDataUpdater.acquireUpdateLockForJoin(updateLock);
            final long elapsedMillis = FDateMillis.nowMillis() - startMillis;
            Assertions.checkTrue(updateLock.isHeldByCurrentThread(), "the joiner ends up owning the lock");
            Assertions.checkTrue(elapsedMillis >= 1_000L,
                    "and it really waited for the holder rather than exiting early: "
                            + Long.valueOf(elapsedMillis) + " ms");
            updateLock.unlock();
        } finally {
            holder.finish();
        }
    }

    @Test
    public void testACallerOnTheOtherTierParksOnTheJoinArmToo() throws InterruptedException {
        //the join arm is chosen by the updater's own policy, not by the caller tier: whoever arrives here came to wait
        //for this update, so marking the thread as a request-tier caller must not turn the join into a refusal
        final ILock updateLock = newLock("join-other-tier");
        final Holder holder = new Holder(updateLock, 1_400);
        holder.start();
        final Boolean previousTier = FinancialdataThreads.registerThreadBlockingUpdateCacheDisabled(true);
        final long startMillis;
        try {
            startMillis = FDateMillis.nowMillis();
            ALazyDataUpdater.acquireUpdateLockForJoin(updateLock);
            final long elapsedMillis = FDateMillis.nowMillis() - startMillis;
            Assertions.checkTrue(updateLock.isHeldByCurrentThread(), "the tier must not change this arm");
            Assertions.checkTrue(elapsedMillis >= 900L,
                    "it waited for the holder rather than being refused at a request-tier ceiling: "
                            + Long.valueOf(elapsedMillis) + " ms");
            updateLock.unlock();
        } finally {
            FinancialdataThreads.unregisterThreadBlockingUpdateCacheDisabled(previousTier);
            holder.finish();
        }
    }

    @Test
    public void testTheDemandArmKeepsItsBoundedAcquire() {
        //the split this change must not dissolve: the live demand path answers "later" and re-asks, so its acquire
        //stays finite - a ceiling exists on that arm, and it is short
        final long ceilingMillis = TimeSeriesProperties.ACQUIRE_UPDATE_LOCK_TIMEOUT
                .longValue(FTimeUnit.MILLISECONDS);
        Assertions.checkTrue(ceilingMillis > 0L,
                "the demand arm must keep a real bound rather than parking: " + Long.valueOf(ceilingMillis) + " ms");
        Assertions.checkTrue(ceilingMillis <= 60_000L, "and it must stay a demand-tier bound, not a disguised long"
                + " wait: " + Long.valueOf(ceilingMillis) + " ms");
    }

    private static ILock newLock(final String name) {
        return Locks.newReentrantLock("StoreUpdateJoinWaitTest_" + name);
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
            }, "StoreUpdateJoinWaitTest-holder");
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
