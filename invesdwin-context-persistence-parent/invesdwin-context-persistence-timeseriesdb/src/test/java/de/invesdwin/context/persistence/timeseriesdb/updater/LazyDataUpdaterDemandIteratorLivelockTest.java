package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.springframework.retry.backoff.BackOffPolicy;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.retry.NonBlockingRetryLaterRuntimeException;
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
 * Blocker-5: the demand read of a key must never wait for that key's update while it is itself holding the key's read
 * side. Every 48-cell matrix worker that needs the derived COMBINE bars is at the same time an iterator holder (its
 * range read opened the key's read side and the demand is issued from inside that read) and a demand waiter (its load
 * joins the update task). The update attempt then refuses with the skip-if-active-iterators verdict - "held by active
 * iterators of another run" - while the iterators that hold it are the demanding readers' own, and the readers keep
 * their read side exactly as long as they wait: nothing can ever close, the store never advances, the demand retries
 * forever (674 skips / 64 retry rounds over 49 minutes in the cap-25 r7 run).
 * <ul>
 * <li>a demand read that holds its OWN read holds on the key must get its update, not the skip verdict - the demand
 * parks its own iterators for the wait and takes them back afterwards (close-before-wait, the same release/re-lock
 * pattern {@code ATimeSeriesUpdater.update()} already applies for its own write acquisition);</li>
 * <li>two such demand readers on one key must both make progress instead of each yielding to the other's iterators
 * (the convoy shape - pre-fix both get the skip and the store stays empty);</li>
 * <li>the guard is not weakened: a reader that is NOT demanding - another run's iterator that keeps the read side -
 * still refuses the attempt with the skip verdict, and the verdict now names the actual read holders instead of
 * asserting "another run" unseen.</li>
 * </ul>
 */
@NotThreadSafe
public class LazyDataUpdaterDemandIteratorLivelockTest extends ATest {

    private static final int BATCH_FLUSH_INTERVAL = 4;
    private static final String KEY = "demandIteratorKey";
    /**
     * Short enough to keep the refusal case cheap, long enough that a parked demand reader always lands inside it.
     */
    private static final Duration ATTEMPT_WRITE_LOCK_WAIT = new Duration(300, FTimeUnit.MILLISECONDS);
    private static final Duration ATTEMPT_BACK_OFF = new Duration(50, FTimeUnit.MILLISECONDS);

    private File baseDirectory;
    private ANestedExecutor nestedExecutor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY,
                LazyDataUpdaterDemandIteratorLivelockTest.class.getSimpleName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
        nestedExecutor = new ANestedExecutor("demandIteratorLivelockTest") {
            @Override
            protected WrappedExecutorService newNestedExecutor(final String nestedName) {
                return Executors.newFixedThreadPool(nestedName, 2);
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
     * The self-blocked shape: the demanding thread holds the key's read side (two open iterators of its own range read)
     * and asks for the update of that same key. The update must land - the demand parks its own read holds for the wait
     * and restores them afterwards - and the hold count must be exactly what it was.
     */
    @Test
    public void testDemandReadWithOwnOpenIteratorsGetsItsUpdate() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("demandSelfHold");
        final List<FDate> values = newValues(2 * BATCH_FLUSH_INTERVAL);
        final TestUpdater updater = new TestUpdater(table, values);
        final ILock readLock = table.getTableLock(KEY).readLock();
        readLock.lock();
        readLock.lock();
        Assertions.checkEquals(2, table.getTableLock(KEY).getReadHoldCount(),
                "the fixture must model an in-progress range read holding the key's read side");
        Throwable thrown = null;
        boolean updated;
        final int[] holdsAfterDemand = new int[1];
        try {
            updated = updater.maybeUpdate(true);
        } catch (final Throwable t) {
            thrown = t;
            updated = false;
        } finally {
            holdsAfterDemand[0] = table.getTableLock(KEY).getReadHoldCount();
            for (int i = 0; i < holdsAfterDemand[0]; i++) {
                readLock.unlock();
            }
        }
        Assertions.checkEquals(2, holdsAfterDemand[0],
                "the demand must take its own read holds back after the wait");
        final NonBlockingRetryLaterRuntimeException refusal = thrown == null ? null
                : Throwables.getCauseByType(thrown, NonBlockingRetryLaterRuntimeException.class);
        Assertions.checkNull(refusal, "a demand read must not be refused by its own open iterators: " + thrown);
        Assertions.checkTrue(updated, "the update of a demanding reader must land: " + describe(updater));
        Assertions.checkEquals(1, updater.landed.get(), "exactly one update attempt must have landed: "
                + describe(updater));
        Assertions.assertThat(readValues(table, KEY)).isEqualTo(new ArrayList<>(values));
    }

    /**
     * The convoy shape: two demand readers, each holding the key's read side, both asking for the update of the same
     * key. Neither may refuse the other - the update has to land and both readers have to proceed. Pre-fix both get the
     * skip-if-active-iterators verdict, no attempt ever runs, and the store stays empty forever (that is the 674-skip
     * livelock of the cap-25 r7 matrix).
     */
    @Test
    public void testTwoDemandReadersWithOpenIteratorsMakeProgress() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("demandConvoy");
        final List<FDate> values = newValues(2 * BATCH_FLUSH_INTERVAL);
        final TestUpdater updater = new TestUpdater(table, values);
        final ILock readLock = table.getTableLock(KEY).readLock();
        final int workers = 2;
        final CountDownLatch ready = new CountDownLatch(workers);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(workers);
        final AtomicInteger refusals = new AtomicInteger();
        final AtomicInteger progressed = new AtomicInteger();
        final AtomicReference<Throwable> firstError = new AtomicReference<>();
        for (int i = 0; i < workers; i++) {
            final Thread worker = new Thread(() -> {
                final ILock workerReadLock = table.getTableLock(KEY).readLock();
                workerReadLock.lock();
                ready.countDown();
                try {
                    if (!start.await(30, TimeUnit.SECONDS)) {
                        return;
                    }
                    if (updater.maybeUpdate(true)) {
                        progressed.incrementAndGet();
                    }
                } catch (final Throwable t) {
                    final NonBlockingRetryLaterRuntimeException refusal = Throwables.getCauseByType(t,
                            NonBlockingRetryLaterRuntimeException.class);
                    if (refusal != null) {
                        refusals.incrementAndGet();
                    } else {
                        //keep the first unexpected failure for the assertion below
                        firstError.compareAndSet(null, t);
                    }
                } finally {
                    workerReadLock.unlock();
                    done.countDown();
                }
            }, "test-demand-convoy-worker-" + i);
            worker.setDaemon(true);
            worker.start();
        }
        Assertions.checkTrue(ready.await(30, TimeUnit.SECONDS), "the demand readers must both be holding the key");
        start.countDown();
        Assertions.checkTrue(done.await(60, TimeUnit.SECONDS),
                "the demand readers must not wait forever on each other's iterators: " + describe(updater));
        Assertions.checkNull(firstError.get(), "a demand reader failed unexpectedly: " + firstError.get());
        Assertions.checkEquals(0, refusals.get(),
                "demand readers must not refuse each other's update: " + describe(updater));
        Assertions.checkTrue(updater.landed.get() >= 1,
                "the update must land while the demanding readers hold their iterators: " + describe(updater));
        Assertions.assertThat(readValues(table, KEY)).isEqualTo(new ArrayList<>(values));
    }

    /**
     * The guard survives: a reader that is only reading - not demanding, another run's iterator - keeps the key's read
     * side, so the attempt must still refuse with the skip verdict after the bounded wait instead of writing into a
     * table that is being iterated. The verdict has to name the actual holders rather than assume "another run".
     */
    @Test
    public void testForeignIteratorStillRefusesTheAttempt() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("demandForeignHold");
        final List<FDate> values = newValues(2 * BATCH_FLUSH_INTERVAL);
        final TestUpdater updater = new TestUpdater(table, values);
        final ILock foreignReadLock = table.getTableLock(KEY).readLock();
        final CountDownLatch foreignHolding = new CountDownLatch(1);
        final Thread foreign = new Thread(() -> {
            foreignReadLock.lock();
            try {
                foreignHolding.countDown();
                while (!Thread.currentThread().isInterrupted()) {
                    Duration.ONE_MILLISECOND.sleepNoInterrupt();
                }
            } finally {
                foreignReadLock.unlock();
            }
        }, "test-demand-foreign-iterator");
        foreign.setDaemon(true);
        foreign.start();
        try {
            Assertions.checkTrue(foreignHolding.await(10, TimeUnit.SECONDS),
                    "the foreign iterator must hold the key's read side before the attempt");
            Throwable thrown;
            try {
                updater.maybeUpdate(true);
                thrown = null;
            } catch (final Throwable t) {
                thrown = t;
            }
            Assertions.checkNotNull(thrown, "a table held by another run's iterators must not let the update pass");
            final NonBlockingRetryLaterRuntimeException refusal = Throwables.getCauseByType(thrown,
                    NonBlockingRetryLaterRuntimeException.class);
            Assertions.checkNotNull(refusal, "the skip-if-readers verdict must reach the caller: " + thrown);
            Assertions.assertThat(refusal.getMessage()).contains("skipped because the table write lock is held by");
            Assertions.checkEquals(0, updater.attempts.get(),
                    "the refusal must run no update body and enter no back-off: " + describe(updater));
        } finally {
            foreign.interrupt();
            foreign.join(30000);
        }
    }

    private static String describe(final TestUpdater updater) {
        return "attempts=" + updater.attempts.get() + ", landed=" + updater.landed.get();
    }

    /**
     * A lazy updater over a real store whose update body always lands the values through {@link #doUpdate(FDate)}; it
     * counts attempts and landings and shortens both the write-lock wait and the back-off so the fixture stays bounded.
     */
    @NotThreadSafe
    private final class TestUpdater extends ALazyDataUpdater<String, FDate> {

        private final ATimeSeriesDB<String, FDate> table;
        private final List<FDate> values;
        private final AtomicInteger attempts = new AtomicInteger();
        private final AtomicInteger landed = new AtomicInteger();

        private TestUpdater(final ATimeSeriesDB<String, FDate> table, final List<FDate> values) {
            super(KEY);
            this.table = table;
            this.values = values;
        }

        @Override
        protected void innerMaybeUpdate(final String key) {
            attempts.incrementAndGet();
            try (TimeSeriesUpdaterResult result = doUpdate(values.get(values.size() - 1))) {
                landed.incrementAndGet();
            } catch (final IncompleteUpdateRetryableException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected Duration newUpdateAttemptWriteLockWait() {
            return ATTEMPT_WRITE_LOCK_WAIT;
        }

        @Override
        protected BackOffPolicy newUpdateAttemptBackOffPolicy() {
            return BackOffPolicies.fixedBackOff(ATTEMPT_BACK_OFF);
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
            return "demand iterator values";
        }

        @Override
        protected FDate extractStartTime(final FDate element) {
            return element;
        }

        @Override
        protected FDate extractEndTime(final FDate element) {
            return element;
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
