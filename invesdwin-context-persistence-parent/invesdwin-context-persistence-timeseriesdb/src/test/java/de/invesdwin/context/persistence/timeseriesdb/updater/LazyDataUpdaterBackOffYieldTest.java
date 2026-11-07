package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.springframework.retry.backoff.BackOffPolicy;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.retry.NonBlockingRetryLaterRuntimeException;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.integration.retry.task.BackOffPolicies;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.nested.ANestedExecutor;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

/**
 * A retrying lazy update attempt must never sit in its back-off while holding the key's table write lock. Demand
 * readers of that key take the table read side with a zero-wait tryLock, so an updater that sleeps with the write lock
 * held does not only delay itself: every reader of the table is refused for the whole sleep, and a source that keeps
 * refusing (a frontier that never advances) keeps refusing them for as long as it keeps retrying. The attempt loop
 * therefore acquires the write lock per attempt and releases it BEFORE backing off, so the back-off window is a window
 * for readers.
 * <ul>
 * <li>a retryable failure of an attempt (the shape of a frontier-pending refusal: an incomplete update whose cause
 * chain names a retry-later) is re-attempted, and a competing reader lands on the write lock inside the back-off
 * window - before the next attempt starts;</li>
 * <li>the skip-if-readers verdict survives the restructure: with another run's iterators holding the key, the caller
 * still gets the refusal immediately - no attempt is run and no back-off is entered;</li>
 * <li>a failure that is not a retry-later stays a hard failure: one attempt, and the cause reaches the caller.</li>
 * </ul>
 */
@NotThreadSafe
public class LazyDataUpdaterBackOffYieldTest extends ATest {

    private static final int BATCH_FLUSH_INTERVAL = 4;
    private static final String KEY = "backOffKey";
    /**
     * Long enough that a reader spinning on the write lock always lands inside the window between two attempts.
     */
    private static final Duration ATTEMPT_BACK_OFF = new Duration(500, FTimeUnit.MILLISECONDS);
    /**
     * Bounded stand-in for UPDATE_ATTEMPT_ACQUIRE_WRITE_LOCK_TIMEOUT so the refusal case stays fast in the fixture.
     */
    private static final Duration ATTEMPT_WRITE_LOCK_WAIT = new Duration(300, FTimeUnit.MILLISECONDS);

    private File baseDirectory;
    private ANestedExecutor nestedExecutor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY,
                LazyDataUpdaterBackOffYieldTest.class.getSimpleName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
        nestedExecutor = new ANestedExecutor("backOffYieldTest") {
            @Override
            protected WrappedExecutorService newNestedExecutor(final String nestedName) {
                return Executors.newFixedThreadPool(nestedName, 1);
            }
        };
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (nestedExecutor != null) {
            nestedExecutor.close();
        }
        Files.deleteQuietly(baseDirectory);
    }

    /**
     * The wedge this rail closes: attempt 1 fails retryably, and while the updater backs off a competing reader must
     * get the table write lock; the re-attempt then finds it free again and lands the values.
     */
    @Test
    public void testBackOffSleepDoesNotHoldTableWriteLock() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("backOffYield");
        final List<FDate> values = newValues(2 * BATCH_FLUSH_INTERVAL);
        final AtomicInteger attempts = new AtomicInteger();
        final CountDownLatch firstAttemptFailed = new CountDownLatch(1);
        final WriteLockProbe probe = new WriteLockProbe(table.getTableLock(KEY).writeLock(), attempts,
                firstAttemptFailed);
        final TestUpdater updater = new TestUpdater(table, values, 1, false, probe, attempts, firstAttemptFailed);

        probe.start();
        try {
            final boolean updated = updater.maybeUpdate(true);
            Assertions.checkTrue(updated, "the re-attempt must complete the update: " + describe(updater, attempts));
        } finally {
            probe.stopAndJoin();
        }

        Assertions.checkEquals(2, attempts.get(),
                "a retryable failure of attempt 1 must be re-attempted: " + describe(updater, attempts));
        Assertions.checkTrue(probe.landed(),
                "a reader must get the table write lock while the updater sits in its back-off: " + describe(updater,
                        attempts));
        Assertions.checkEquals(1, probe.attemptsWhenLanded(),
                "the reader must have landed between attempt 1 and the re-attempt, not with "
                        + probe.attemptsWhenLanded() + " attempts in flight");
        Assertions.assertThat(readValues(table, KEY)).isEqualTo(new ArrayList<>(values));
    }

    /**
     * The skip-if-readers verdict is not swallowed by the attempt loop: while ANOTHER run keeps the key's read side, the
     * caller gets the refusal once the attempt's bounded write-lock wait runs out - the update body never ran and no
     * back-off was started. (The holder is a separate on purpose: the demanding thread's own read holds are no longer a
     * refusal reason, they are parked for the wait - see LazyDataUpdaterDemandIteratorLivelockTest.)
     */
    @Test
    public void testActiveIteratorsStillRefuseWithoutBackingOff() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("backOffSkip");
        final List<FDate> values = newValues(2 * BATCH_FLUSH_INTERVAL);
        final ILock readLock = table.getTableLock(KEY).readLock();
        final CountDownLatch held = new CountDownLatch(1);
        final Thread foreignIterator = new Thread(() -> {
            readLock.lock();
            try {
                held.countDown();
                while (!Thread.currentThread().isInterrupted()) {
                    Duration.ONE_MILLISECOND.sleepNoInterrupt();
                }
            } finally {
                readLock.unlock();
            }
        }, "test-back-off-yield-foreign-iterator");
        foreignIterator.setDaemon(true);
        foreignIterator.start();
        final AtomicInteger attempts = new AtomicInteger();
        final CountDownLatch firstAttemptFailed = new CountDownLatch(1);
        final TestUpdater updater = new TestUpdater(table, values, 1, false,
                new WriteLockProbe(table.getTableLock(KEY).writeLock(), attempts, firstAttemptFailed), attempts,
                firstAttemptFailed);
        Throwable thrown;
        try {
            Assertions.checkTrue(held.await(10, TimeUnit.SECONDS),
                    "the competing run's iterators must hold the key's read side before the attempt");
            updater.maybeUpdate(true);
            thrown = null;
        } catch (final Throwable t) {
            thrown = t;
        } finally {
            foreignIterator.interrupt();
            foreignIterator.join(30000);
        }

        Assertions.checkNotNull(thrown, "a table held by active iterators must not let the update pass");
        final NonBlockingRetryLaterRuntimeException refusal = Throwables.getCauseByType(thrown,
                NonBlockingRetryLaterRuntimeException.class);
        Assertions.checkNotNull(refusal, "the skip-if-readers verdict must reach the caller: " + thrown);
        Assertions.assertThat(refusal.getMessage()).contains(
                "skipped because the table write lock is held by active iterators");
        Assertions.checkEquals(0, attempts.get(),
                "the refusal must be immediate - no attempt ran and no back-off loop started");
    }

    /**
     * A failure that is not a retry-later stays a hard failure: exactly one attempt, and the cause reaches the caller
     * instead of being retried behind the table write lock.
     */
    @Test
    public void testNonRetryableFailureIsNotRetried() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("backOffFatal");
        final List<FDate> values = newValues(2 * BATCH_FLUSH_INTERVAL);
        final AtomicInteger attempts = new AtomicInteger();
        final CountDownLatch firstAttemptFailed = new CountDownLatch(1);
        final TestUpdater updater = new TestUpdater(table, values, 0, true,
                new WriteLockProbe(table.getTableLock(KEY).writeLock(), attempts, firstAttemptFailed), attempts,
                firstAttemptFailed);
        Throwable thrown;
        try {
            updater.maybeUpdate(true);
            thrown = null;
        } catch (final Throwable t) {
            thrown = t;
        }

        Assertions.checkNotNull(thrown, "a hard failure of the update body must reach the caller");
        final IllegalStateException hardFailure = Throwables.getCauseByType(thrown, IllegalStateException.class);
        Assertions.checkNotNull(hardFailure, "the update body's failure must reach the caller: " + thrown);
        Assertions.checkEquals(1, attempts.get(), "a non-retryable failure must not be re-attempted");
        Assertions.assertThat(readValues(table, KEY)).isEmpty();
    }

    private static String describe(final TestUpdater updater, final AtomicInteger attempts) {
        return "attempts=" + attempts.get() + ", updated=" + updater.lastUpdateCheckSeen();
    }

    /**
     * A lazy updater over a real store: its first {@code retryableFailures} update bodies fail the way a refusing
     * source does (an incomplete update whose cause chain names a retry-later), a later attempt lands the values
     * through {@link #doUpdate(FDate)}. With {@code hardFailureOnFirstAttempt} the first attempt dies with an error
     * that is not a retry-later instead.
     */
    @NotThreadSafe
    private final class TestUpdater extends ALazyDataUpdater<String, FDate> {

        private final ATimeSeriesDB<String, FDate> table;
        private final List<FDate> values;
        private final int retryableFailures;
        private final boolean hardFailureOnFirstAttempt;
        private final WriteLockProbe probe;
        private final AtomicInteger attempts;
        private final CountDownLatch firstAttemptFailed;
        private volatile boolean lastUpdateCheckSeen;

        private TestUpdater(final ATimeSeriesDB<String, FDate> table, final List<FDate> values,
                final int retryableFailures, final boolean hardFailureOnFirstAttempt, final WriteLockProbe probe,
                final AtomicInteger attempts, final CountDownLatch firstAttemptFailed) {
            super(KEY);
            this.table = table;
            this.values = values;
            this.retryableFailures = retryableFailures;
            this.hardFailureOnFirstAttempt = hardFailureOnFirstAttempt;
            this.probe = probe;
            this.attempts = attempts;
            this.firstAttemptFailed = firstAttemptFailed;
        }

        @Override
        protected void innerMaybeUpdate(final String key) {
            final int attempt = attempts.incrementAndGet();
            if (hardFailureOnFirstAttempt) {
                throw new IllegalStateException("simulated hard failure of the update body");
            }
            if (attempt <= retryableFailures) {
                firstAttemptFailed.countDown();
                throw new RuntimeException(new IncompleteUpdateRetryableException(
                        "the requested range demands beyond the published store frontier - frontier pending",
                        new RetryLaterRuntimeException("the store backfill has not reached the demand yet")));
            }
            //the re-attempt may only start once the reader that landed in the back-off window is done with the table;
            //if the updater had slept WITH the write lock held, the reader never lands and this is where it shows up
            Assertions.checkTrue(probe.awaitReleased(3, TimeUnit.SECONDS),
                    "attempt " + attempt + " must find the table write lock free - no reader landed in the back-off"
                            + " window (attempts in flight: " + attempts.get() + ")");
            try (TimeSeriesUpdaterResult result = doUpdate(values.get(values.size() - 1))) {
                lastUpdateCheckSeen = true;
            } catch (final IncompleteUpdateRetryableException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected BackOffPolicy newUpdateAttemptBackOffPolicy() {
            return BackOffPolicies.fixedBackOff(ATTEMPT_BACK_OFF);
        }

        @Override
        protected Duration newUpdateAttemptWriteLockWait() {
            //the fixture stays fast: the foreign-holder case only has to prove the refusal still arrives, not that it
            //waits out the production ceiling of UPDATE_ATTEMPT_ACQUIRE_WRITE_LOCK_TIMEOUT
            return ATTEMPT_WRITE_LOCK_WAIT;
        }

        @Override
        protected ANestedExecutor getNestedExecutor() {
            return nestedExecutor;
        }

        @Override
        protected ATimeSeriesDB<String, FDate> getTable() {
            return table;
        }

        @Override
        protected ICloseableIterable<? extends FDate> downloadElements(final String key, final FDate fromDate) {
            return WrapperCloseableIterable.maybeWrap(values);
        }

        @Override
        protected String getElementsName() {
            return "back off yield values";
        }

        @Override
        protected FDate extractStartTime(final FDate element) {
            return element;
        }

        @Override
        protected FDate extractEndTime(final FDate element) {
            return element;
        }

        private boolean lastUpdateCheckSeen() {
            return lastUpdateCheckSeen;
        }
    }

    /**
     * A competing reader of the key's table write lock: it starts spinning the moment the failing update attempt
     * signals it (so it races the back-off window and not the attempt before it), records how many attempts were in
     * flight when it landed, holds the lock briefly and releases it again.
     */
    @NotThreadSafe
    private static final class WriteLockProbe {

        private static final long HOLD_MILLIS = 20;

        private final ILock writeLock;
        private final AtomicInteger attempts;
        private final CountDownLatch startSignal;
        private final CountDownLatch landed = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);
        private final AtomicInteger attemptsWhenLanded = new AtomicInteger(-1);
        private final Thread thread;
        private volatile boolean stopped;

        private WriteLockProbe(final ILock writeLock, final AtomicInteger attempts, final CountDownLatch startSignal) {
            this.writeLock = writeLock;
            this.attempts = attempts;
            this.startSignal = startSignal;
            this.thread = new Thread(this::run, "test-back-off-yield-write-lock-probe");
            this.thread.setDaemon(true);
        }

        private void start() {
            thread.start();
        }

        private void run() {
            try {
                if (!startSignal.await(60, TimeUnit.SECONDS)) {
                    return;
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            boolean acquired = false;
            while (!stopped) {
                if (writeLock.tryLock()) {
                    acquired = true;
                    break;
                }
                Duration.ONE_MILLISECOND.sleepNoInterrupt();
            }
            if (!acquired) {
                return;
            }
            attemptsWhenLanded.set(attempts.get());
            landed.countDown();
            for (long i = 0; i < HOLD_MILLIS; i++) {
                Duration.ONE_MILLISECOND.sleepNoInterrupt();
            }
            writeLock.unlock();
            released.countDown();
        }

        private boolean landed() {
            return landed.getCount() == 0;
        }

        private int attemptsWhenLanded() {
            return attemptsWhenLanded.get();
        }

        private boolean awaitReleased(final long timeout, final TimeUnit unit) {
            try {
                return released.await(timeout, unit);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        private void stopAndJoin() {
            stopped = true;
            startSignal.countDown();
            try {
                thread.join(30000);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static List<FDate> newValues(final int count) {
        final List<FDate> values = new ArrayList<>();
        for (int day = 1; day <= count; day++) {
            values.add(FDateBuilder.newDate(2020, 1, 1).addDays(day));
        }
        return values;
    }

    private static List<FDate> readValues(final ATimeSeriesDB<String, FDate> table, final String key) {
        final List<FDate> values = new ArrayList<>();
        try (ICloseableIterator<FDate> it = table.rangeValues(key, FDates.MIN_DATE, FDates.MAX_DATE).iterator()) {
            Lists.toListWithoutHasNext(it, values);
        }
        return values;
    }

    private ATimeSeriesDB<String, FDate> newTable(final String name) {
        return new ATimeSeriesDB<String, FDate>(name) {

            @Override
            protected ISerde<FDate> newValueSerde() {
                return FDateSerde.GET;
            }

            @Override
            protected Integer newValueFixedLength() {
                return FDateSerde.FIXED_LENGTH;
            }

            @Override
            protected int newBatchFlushInterval() {
                return BATCH_FLUSH_INTERVAL;
            }

            @Override
            public ITimeSeriesBaseDirectory getBaseDirectory() {
                return new TimeSeriesBaseDirectory(baseDirectory);
            }

            @Override
            public String innerHashKeyToString(final String key) {
                return key;
            }

            @Override
            public FDate extractStartTime(final FDate value) {
                return value;
            }

            @Override
            public FDate extractEndTime(final FDate value) {
                return value;
            }
        };
    }

}
