package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateYieldException;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFiles;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdateProgress;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.date.millis.FDateMillis;
import de.invesdwin.util.time.duration.Duration;

/**
 * Publish-then-yield (write-lock convoy): while an update attempt holds the key's table write lock, a competing reader
 * must not be frozen out for the whole pass - every boundary commit releases the write lock and re-acquires it within
 * a bounded wait. A reader that keeps the read side past that window ENDS the attempt cleanly at the published
 * boundary (never mid-file), the visible store in the released window is exactly the published generations, and the
 * published prefix is the resume point, so the clean end loses no progress: the next attempt of the key continues from
 * it. A reader that lands and leaves again only costs the writer at most one boundary batch of waiting - the attempt
 * takes the lock back and runs the source through.
 */
@NotThreadSafe
public class TimeSeriesWriteLockYieldTest extends ATest {

    private static final int BATCH_FLUSH_INTERVAL = 4;
    private static final String KEY = "yieldKey";

    private File baseDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY,
                TimeSeriesWriteLockYieldTest.class.getSimpleName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Files.deleteQuietly(baseDirectory);
    }

    /**
     * A reader that holds the key's read side continuously must force the attempt to end cleanly at its first
     * boundary commit: exactly one boundary published, nothing staged beyond it visible, the store not mid-file, no
     * litter, and the next attempt resumes from the published prefix with every value landing exactly once.
     */
    @Test
    public void testContinuousReaderEndsAttemptCleanlyAtPublishedBoundary() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("yieldEnd");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final int commitInterval = TimeSeriesUpdateTransaction.PUBLISH_SEGMENTS_PER_GENERATION;
        final int boundaryValues = commitInterval * BATCH_FLUSH_INTERVAL;
        final int totalValues = 5 * boundaryValues + BATCH_FLUSH_INTERVAL;
        final List<FDate> allValues = newValues(1, totalValues);

        final CountedSource source = new CountedSource(allValues, false);
        final ATimeSeriesUpdater<String, FDate> yielding = newUpdater(table, KEY, source);
        final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        final AtomicBoolean writerCompleted = new AtomicBoolean();
        final Thread writer = newUpdateThread(yielding, writerFailure, writerCompleted);
        writer.start();
        //the attempt holds the table write lock for its pass once it started consuming its source
        Assertions.checkTrue(source.awaitConsumed(BATCH_FLUSH_INTERVAL, 30000),
                "the attempt must be mid-pass: " + describeWriter(writer, writerFailure, writerCompleted));
        //a competing reader requests the read side while the write lock is held and keeps it from then on
        final ReadLockContender reader = new ReadLockContender(table.getTableLock(KEY).readLock(), -1);
        reader.start();
        Assertions.checkTrue(reader.awaitAcquired(30000),
                "a boundary commit must release the table write lock so the competing reader gets its window");
        //while the attempt sits in its bounded re-acquisition window with the reader holding the read side, the
        //visible store is exactly the published generations: one boundary, nothing staged beyond it, nothing mid-file
        final List<FDate> visibleInWindow = readValues(table, KEY);
        Assertions.checkEquals(boundaryValues, visibleInWindow.size(),
                "the released window must expose exactly the published prefix, not staged state");
        writer.join(60000);
        Assertions.checkFalse(writer.isAlive(), "the attempt must end at the boundary, not linger on the source");
        Assertions.checkFalse(writerCompleted.get(),
                "a reader holding the key past the re-acquisition window must end the attempt, not let it run the source through");
        final Throwable failure = writerFailure.get();
        Assertions.checkTrue(failure instanceof IncompleteUpdateRetryableException,
                "the clean yield end must surface as a retryable incomplete update: " + failure);
        Assertions.checkNotNull(Throwables.getCauseByType(failure, IncompleteUpdateYieldException.class),
                "the retryable exception must identify the write lock yield: " + failure);

        //the attempt ended AT the boundary: one boundary's segments published, the rest of the source never fetched
        final List<MemoryFileSummary> published = readPublishedSummaries(lookup);
        Assertions.checkEquals(commitInterval, published.size(),
                "the attempt must stop at its first boundary commit: " + published);
        Assertions.assertThat(readValues(table, KEY)).isEqualTo(new ArrayList<>(
                allValues.subList(0, boundaryValues)));
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "a clean end at a boundary must not leave the store mid-file: " + published);

        //no litter beyond the per-file reclaim semantics of the resumable attempt: of the ended attempt's data files
        //only the ones a published generation names survive, no '_incomplete_' file, no staging index left behind
        final Set<String> referencedNames = ILockCollectionFactory.getInstance(false).newLinkedSet();
        for (final MemoryFileSummary summary : published) {
            referencedNames.add(new File(summary.getMemoryResourceUri()).getName());
        }
        for (final File leftover : findAttemptDataFiles(lookup, yielding.getEpochId())) {
            Assertions.checkTrue(referencedNames.contains(leftover.getName()),
                    "the yielded attempt left an unreferenced data file behind: " + leftover);
            Assertions.checkFalse(MemoryFiles.isIncompleteMemoryFile(leftover.getName()),
                    "a yielded attempt must not leave '_incomplete_' litter behind: " + leftover);
        }
        checkNoStagingIndexLitter(lookup);
        final byte[] publishedBytes = readAllBytes(new File(published.get(0).getMemoryResourceUri()));

        //the reader parked: the next attempt resumes from the published prefix (zero progress lost at the yield)
        reader.releaseHold();
        reader.join(30000);
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(allValues)).update().close();
        Assertions.assertThat(readValues(table, KEY)).isEqualTo(new ArrayList<>(allValues));
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "the store must stay consistent over the resume: " + readPublishedSummaries(lookup));
        final List<MemoryFileSummary> resumed = readPublishedSummaries(lookup);
        for (int i = 0; i < published.size(); i++) {
            Assertions.checkEquals(published.get(i), resumed.get(i),
                    i + ": the resume must not change an already published summary");
        }
        final File firstPublishedFile = new File(published.get(0).getMemoryResourceUri());
        Assertions.checkTrue(firstPublishedFile.exists(), "the resume removed a published data file: "
                + firstPublishedFile);
        Assertions.checkTrue(Arrays.equals(publishedBytes, readAllBytes(firstPublishedFile)),
                "the resume mutated the bytes of a published data file: " + firstPublishedFile);
        checkNoStagingIndexLitter(lookup);
    }

    /**
     * A reader that lands at a boundary commit and leaves again must wait at most one boundary batch for its window,
     * and the attempt must then take the write lock back (bounded re-acquisition) and run the source through -
     * writer and readers coexist on the key instead of the writer convoying on it.
     */
    @Test
    public void testReaderNeverBlockedLongerThanOneBoundaryBatch() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("yieldWindow");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final int commitInterval = TimeSeriesUpdateTransaction.PUBLISH_SEGMENTS_PER_GENERATION;
        final int boundaryValues = commitInterval * BATCH_FLUSH_INTERVAL;
        final int totalValues = 3 * boundaryValues + BATCH_FLUSH_INTERVAL;
        final int totalBatches = totalValues / BATCH_FLUSH_INTERVAL;
        final List<FDate> allValues = newValues(1, totalValues);

        //the source is fed batch by batch, so how many batches the writer could pass after the reader asked for the
        //lock is exactly how long the reader was blocked
        final CountedSource source = new CountedSource(allValues, true);
        final ATimeSeriesUpdater<String, FDate> updater = newUpdater(table, KEY, source);
        final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        final AtomicBoolean writerCompleted = new AtomicBoolean();
        final Thread writer = newUpdateThread(updater, writerFailure, writerCompleted);
        writer.start();
        source.feedBatch();
        Assertions.checkTrue(source.awaitConsumed(BATCH_FLUSH_INTERVAL, 30000),
                "the attempt must be mid-pass: " + describeWriter(writer, writerFailure, writerCompleted));
        //the reader asks for the read side mid-batch and holds it only briefly (a report iterator landing)
        final ReadLockContender reader = new ReadLockContender(table.getTableLock(KEY).readLock(), 20);
        reader.start();
        int fedBatches = 1;
        while (!reader.awaitAcquired(100) && fedBatches < totalBatches) {
            source.feedBatch();
            fedBatches++;
        }
        Assertions.checkTrue(reader.awaitAcquired(0),
                "the reader must get its window at a boundary commit, not only when the whole pass ends");
        Assertions.checkTrue(fedBatches <= commitInterval + 2,
                "the reader waited across " + fedBatches
                        + " batches - the boundary commit must release the write lock within one boundary batch");
        //the attempt re-acquires the write lock and runs the rest of the source through to the end
        while (!writerCompleted.get() && writerFailure.get() == null && source.hasRemaining()) {
            source.feedBatch();
        }
        writer.join(60000);
        Assertions.checkFalse(writer.isAlive(), "the attempt must finish once the source is fully fed");
        Assertions.checkTrue(writerCompleted.get(),
                "after yielding and re-acquiring the attempt must run to completion: " + writerFailure.get());
        reader.join(30000);
        Assertions.assertThat(readValues(table, KEY)).isEqualTo(new ArrayList<>(allValues));
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "the store must be a consistent, complete store after the coexisting pass: "
                        + readPublishedSummaries(lookup));
        checkNoStagingIndexLitter(lookup);
    }

    private static String describeWriter(final Thread writer, final AtomicReference<Throwable> failure,
            final AtomicBoolean completed) {
        final StringBuilder sb = new StringBuilder();
        sb.append("writerAlive=").append(writer.isAlive()).append(", completed=").append(completed.get());
        final Throwable t = failure.get();
        if (t != null) {
            sb.append(", failure=").append(t);
            for (final StackTraceElement element : t.getStackTrace()) {
                if (element.getClassName().startsWith("de.invesdwin")) {
                    sb.append("\n\tat ").append(element);
                }
            }
        }
        return sb.toString();
    }

    private Thread newUpdateThread(final ATimeSeriesUpdater<String, FDate> updater,
            final AtomicReference<Throwable> failure, final AtomicBoolean completed) {
        return new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    updater.update().close();
                    completed.set(true);
                } catch (final Throwable t) {
                    failure.set(t);
                }
            }
        }, "test-write-lock-yield-writer");
    }

    /**
     * A reader of the key's read side: requests it with a short-timeout tryLock spin (it lands at the first moment
     * the write lock is released), records that it landed, and then keeps it either until {@link #releaseHold()}
     * (holdMillis &lt; 0, the continuously iterating report) or for {@code holdMillis} (a report that lands and
     * leaves).
     */
    @NotThreadSafe
    private static final class ReadLockContender {

        private final ILock readLock;
        private final long holdMillis;
        private final CountDownLatch acquired = new CountDownLatch(1);
        private final CountDownLatch releaseHold = new CountDownLatch(1);
        private final Thread thread;

        private ReadLockContender(final ILock readLock, final long holdMillis) {
            this.readLock = readLock;
            this.holdMillis = holdMillis;
            this.thread = new Thread(new Runnable() {

                @Override
                public void run() {
                    while (!readLock.tryLock()) {
                        Duration.ONE_MILLISECOND.sleepNoInterrupt();
                    }
                    acquired.countDown();
                    try {
                        if (holdMillis >= 0) {
                            for (long i = 0; i < holdMillis; i++) {
                                Duration.ONE_MILLISECOND.sleepNoInterrupt();
                            }
                        } else {
                            awaitReleaseHold();
                        }
                    } finally {
                        readLock.unlock();
                    }
                }
            }, "test-write-lock-yield-reader");
        }

        private void awaitReleaseHold() {
            try {
                releaseHold.await(60, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private void start() {
            thread.start();
        }

        private boolean awaitAcquired(final long timeoutMillis) {
            try {
                return acquired.await(timeoutMillis, TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        private void releaseHold() {
            releaseHold.countDown();
        }

        private void join(final long timeoutMillis) {
            try {
                thread.join(timeoutMillis);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * A source that counts what it handed out (the test observes the attempt is mid-pass before the reader asks for
     * the lock) and either paces itself like a feed at about a millisecond per value or hands values out only batch
     * by batch as the test feeds them (the reader block time then equals the number of batches fed past the request).
     */
    @NotThreadSafe
    private static final class CountedSource implements ICloseableIterable<FDate> {

        private final List<FDate> values;
        private final boolean gated;
        private final Semaphore permits;
        private final AtomicInteger consumed = new AtomicInteger();

        private CountedSource(final List<FDate> values, final boolean gated) {
            this.values = values;
            this.gated = gated;
            this.permits = gated ? new Semaphore(0) : null;
        }

        private void feedBatch() {
            if (gated) {
                permits.release(Integers.min(BATCH_FLUSH_INTERVAL, values.size() - consumed.get()));
            }
        }

        private boolean hasRemaining() {
            return consumed.get() < values.size();
        }

        private boolean awaitConsumed(final int count, final long timeoutMillis) {
            final long deadline = FDateMillis.nowMillis() + timeoutMillis;
            while (consumed.get() < count) {
                if (FDateMillis.nowMillis() > deadline) {
                    return false;
                }
                Duration.ONE_MILLISECOND.sleepNoInterrupt();
            }
            return true;
        }

        @Override
        public ICloseableIterator<FDate> iterator() {
            return new ICloseableIterator<FDate>() {

                private int index = 0;

                @Override
                public boolean hasNext() {
                    return index < values.size();
                }

                @Override
                public FDate next() {
                    if (index >= values.size()) {
                        throw FastNoSuchElementException.getInstance(
                                TimeSeriesWriteLockYieldTest.class.getSimpleName() + ": counted source end reached");
                    }
                    if (gated) {
                        try {
                            permits.acquire();
                        } catch (final InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        Duration.ONE_MILLISECOND.sleepNoInterrupt();
                    }
                    final FDate value = values.get(index++);
                    consumed.incrementAndGet();
                    return value;
                }

                @Override
                public void close() {
                    if (gated) {
                        //never let a stalled pass hang on a permit when it is being torn down
                        permits.release(values.size());
                    }
                }
            };
        }
    }

    private static void checkNoStagingIndexLitter(final TimeSeriesLookupStorageCache<String, FDate> lookup) {
        final File dataDirectory = lookup.getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        final File[] litter = dataDirectory.listFiles((dir, name) -> name.endsWith(".update.tmp"));
        final List<String> litterNames = new ArrayList<>();
        if (litter != null) {
            for (final File file : litter) {
                litterNames.add(file.getName());
            }
        }
        Assertions.checkTrue(litterNames.isEmpty(),
                "the yielded attempt must not leave its staging index behind: " + litterNames);
    }

    private static List<File> findAttemptDataFiles(final TimeSeriesLookupStorageCache<String, FDate> lookup,
            final String attemptId) {
        final File dataDirectory = lookup.getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        final File[] files = dataDirectory
                .listFiles((dir, name) -> name.startsWith(MemoryFiles.EPOCH_LOCAL_DATA_FILE_PREFIX + attemptId));
        final List<File> found = new ArrayList<>();
        if (files != null) {
            for (final File file : files) {
                found.add(file);
            }
        }
        return found;
    }

    private static List<FDate> newValues(final int fromDay, final int toDay) {
        final List<FDate> values = new ArrayList<>();
        for (int day = fromDay; day <= toDay; day++) {
            values.add(FDateBuilder.newDate(2020, 1, 1).addDays(day));
        }
        return values;
    }

    private static List<MemoryFileSummary> readPublishedSummaries(
            final TimeSeriesLookupStorageCache<String, FDate> lookupTable) {
        final List<MemoryFileSummary> summaries = new ArrayList<>();
        try (ICloseableIterator<MemoryFileSummary> it = lookupTable.getMemoryFileLookupTable().range()) {
            Lists.toListWithoutHasNext(it, summaries);
        }
        return summaries;
    }

    private static List<FDate> readValues(final ATimeSeriesDB<String, FDate> table, final String key) {
        final List<FDate> values = new ArrayList<>();
        try (ICloseableIterator<FDate> it = table.rangeValues(key, FDates.MIN_DATE, FDates.MAX_DATE).iterator()) {
            Lists.toListWithoutHasNext(it, values);
        }
        return values;
    }

    /**
     * java.nio is spelled out because this file imports the invesdwin {@link Files} helper for the store directories.
     */
    private static byte[] readAllBytes(final File file) throws Exception {
        return java.nio.file.Files.readAllBytes(file.toPath());
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

    private ATimeSeriesUpdater<String, FDate> newUpdater(final ATimeSeriesDB<String, FDate> table, final String key,
            final ICloseableIterable<? extends FDate> source) {
        return new ATimeSeriesUpdater<String, FDate>(key, table) {

            @Override
            protected ICloseableIterable<? extends FDate> getSource(final FDate updateFrom) {
                return source;
            }

            @Override
            protected boolean shouldWriteInParallel() {
                return false;
            }

            @Override
            protected void onUpdateStarted(final FDate updateStart) {}

            @Override
            protected void onUpdateFinished() {}

            @Override
            protected FDate extractStartTime(final FDate element) {
                return element;
            }

            @Override
            protected FDate extractEndTime(final FDate element) {
                return element;
            }

            @Override
            protected void onElement(final ITimeSeriesUpdateProgress relativeProgress, final long relativeCount) {}

            @Override
            protected void onFlush(final ITimeSeriesUpdateProgress relativeProgress, final long flushIndex) {}

            @Override
            public Percent getProgress(final FDate minTime, final FDate maxTime) {
                return null;
            }
        };
    }

}
