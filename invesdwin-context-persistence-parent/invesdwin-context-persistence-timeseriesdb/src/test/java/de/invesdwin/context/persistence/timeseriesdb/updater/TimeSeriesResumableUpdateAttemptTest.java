package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.ITimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFiles;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.MemoryFileMetadata;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.VersionedTimeSeriesMemoryFileLookupTable;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdateProgress;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdaterInternalMethods;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.SequentialChunkedUpdateProgress;
import de.invesdwin.context.system.properties.ICloseableProperties;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.collections.iterable.skip.ASkippingIterable;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.string.Charsets;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FDates;

/**
 * An update attempt that dies mid-pass (a source that times out or is cancelled) must not throw away the bytes it has
 * already landed. Two publishes keep that honest: a boundary commit every
 * {@link TimeSeriesProperties#UPDATE_COMMIT_SEGMENT_INTERVAL} completed segments, which gives a pass that runs for
 * hours a readable high-water mark WHILE it runs, and the publish {@code close()} does with everything the attempt
 * staged after its last boundary - committed or aborted alike, because a finished data file is complete content. What
 * an abort can lose is the one batch that was still open when the source died, and the epoch reclaim deletes that file
 * - no '_incomplete_' litter accumulates. The next attempt of the key is then told to continue from the published
 * frontier instead of being asked to re-fetch the whole range.
 */
@NotThreadSafe
public class TimeSeriesResumableUpdateAttemptTest extends ATest {

    private static final int BATCH_FLUSH_INTERVAL = 4;
    private static final String KEY = "resumableKey";

    private File baseDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY,
                TimeSeriesResumableUpdateAttemptTest.class.getSimpleName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Files.deleteQuietly(baseDirectory);
    }

    @Test
    public void testAbortedAttemptResumesFromPublishedHighWater() throws Exception {
        checkAbortedAttemptResumes(false);
    }

    @Test
    public void testAbortedAttemptResumesFromPublishedHighWaterInParallel() throws Exception {
        checkAbortedAttemptResumes(true);
    }

    private void checkAbortedAttemptResumes(final boolean parallel) throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("resume" + parallel);
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final int commitInterval = TimeSeriesProperties.UPDATE_COMMIT_SEGMENT_INTERVAL;
        //a parallel writer feeds its queue ahead of the writer loop, so the pass has to die well after the first
        //boundary commit for that boundary to be published when the source dies
        final int firstBoundaryValues = commitInterval * BATCH_FLUSH_INTERVAL;
        final int landedValues = 3 * firstBoundaryValues + BATCH_FLUSH_INTERVAL;
        final int totalValues = landedValues + BATCH_FLUSH_INTERVAL;
        final List<FDate> allValues = newValues(1, totalValues);

        //the witness list fills up as the attempt publishes boundaries; the source waits for its first entry before
        //it dies, so both writer shapes are pinned against the publish rather than against the clock
        final List<FDate> publishedBoundaries = new ArrayList<>();
        final ATimeSeriesUpdater<String, FDate> aborting = newUpdater(table, KEY,
                abortingSource(allValues, landedValues, publishedBoundaries), parallel, null, publishedBoundaries);
        boolean aborted = false;
        try {
            aborting.update().close();
        } catch (final IncompleteUpdateRetryableException e) {
            aborted = true;
        }
        Assertions.checkTrue(aborted, "the aborted pass must propagate");
        final String abortedAttemptId = aborting.getEpochId();
        Assertions.checkNotNull(abortedAttemptId, "the aborted attempt must have minted its data file id");

        //every segment the attempt FINISHED before its source died is published, not only the boundary commits: a
        //finished data file is complete content, so an aborted pass loses at most the one batch it was writing
        final List<MemoryFileSummary> published = readPublishedSummaries(lookup);
        Assertions.checkTrue(!published.isEmpty(),
                "the boundary commit must have published the segments it completed before the abort");
        final List<FDate> publishedValues = readValues(table, KEY);
        Assertions.checkTrue(publishedValues.size() >= firstBoundaryValues,
                "at least one boundary must be readable after the abort, got: " + publishedValues.size());
        Assertions.checkTrue(publishedValues.size() <= landedValues,
                "the abort must not publish values the source never handed out: " + publishedValues.size());
        if (!parallel) {
            //The sequential writer dies with its source, so at most the one batch it was writing can be missing; this
            //fixture lands whole batches, so every landed value is published by the close. The parallel writer cannot
            //be pinned to a count: its producer races ahead of the writer loop, so the queue can die with several
            //batches never written to a file. What BOTH shapes pin is below - no file the attempt finished stays
            //unpublished, and no '_incomplete_' residue.
            Assertions.checkEquals(publishedValues.size(), landedValues,
                    "an aborted sequential pass must publish every batch it finished: " + publishedValues.size());
        }
        Assertions.assertThat(publishedValues).isEqualTo(new ArrayList<>(
                allValues.subList(0, publishedValues.size())));
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "the store must not report itself inconsistent after an aborted attempt: " + published);

        //the files of the aborted attempt that survive are exactly the ones the published generation names: the
        //staged-but-unpublished tail of the attempt leaves no litter behind
        final Set<String> referencedNames = ILockCollectionFactory.getInstance(false).newLinkedSet();
        for (final MemoryFileSummary summary : published) {
            referencedNames.add(new File(summary.getMemoryResourceUri()).getName());
        }
        for (final File leftover : findAttemptDataFiles(lookup, abortedAttemptId)) {
            Assertions.checkTrue(referencedNames.contains(leftover.getName()),
                    "the aborted attempt left an unreferenced data file behind: " + leftover);
            Assertions.checkFalse(MemoryFiles.isIncompleteMemoryFile(leftover.getName()),
                    "an aborted attempt must not leave '_incomplete_' litter behind: " + leftover);
        }
        final byte[] publishedBytes = readAllBytes(new File(published.get(0).getMemoryResourceUri()));

        //the next attempt continues where the aborted one stopped: the values it re-fetches are the ones that were
        //never published, nothing published before is rewritten or lost. The demand it is given is the payoff the
        //fleet pays for here: the store's frontier is the last value the ABORT landed, so the source is asked to
        //supply from there and not to replay the range the aborted attempt already wrote (the live box paid 595 full
        //replays of a 2006 to 2021 pass for an attempt whose work was discarded).
        final AtomicReference<FDate> resumeUpdateFrom = new AtomicReference<>();
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(allValues), parallel, resumeUpdateFrom)
                .update().close();
        Assertions.assertThat(resumeUpdateFrom.get())
                .as("the resume must demand from the frontier the abort published")
                .isEqualTo(publishedValues.get(publishedValues.size() - 1));
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
    }

    /**
     * One {@code memory.log} record per update attempt - committed OR aborted - stating what the store holds when the
     * attempt closes, and nothing at all for an attempt that lands nothing new. The per-summary shape this replaces
     * measured 3,139 MB across 6,535 keys on ptstaging (largest single file 42.2 MB with 73,139 records), which answers
     * far less than one attempt record does: the attempt id, the outcome, how many data files it staged, how many times
     * it published and the published frontier.
     */
    @Test
    public void testEachAttemptLeavesOneLogRecordStatingWhatTheStoreHolds() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("logPerAttempt");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final File memoryDir = lookup.getDirectoryHashKeyVersionMemory().getDirectoryHashKeyVersionDataShared();
        final File logFile = new File(memoryDir, "memory.log");
        final int boundaryValues = TimeSeriesProperties.UPDATE_COMMIT_SEGMENT_INTERVAL * BATCH_FLUSH_INTERVAL;
        final List<FDate> allValues = newValues(1, 3 * boundaryValues);

        boolean aborted = false;
        try {
            newUpdater(table, KEY, abortingSource(allValues, boundaryValues), false).update().close();
        } catch (final IncompleteUpdateRetryableException e) {
            aborted = true;
        }
        Assertions.checkTrue(aborted, "the pass has to die mid-way for this test to mean anything");
        String log = Files.readFileToString(logFile, Charsets.UTF_8);
        Assertions.assertThat(countAttemptRecords(log))
                .as("an attempt that dies mid-pass leaves exactly one record")
                .isEqualTo(1);
        Assertions.assertThat(log)
                .as("and that record states the outcome and what was landed")
                .contains("OUTCOME=ABORTED").contains("DATA_FILES_STAGED=").contains("EPOCH_LOCAL_DATA_ID=");

        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(allValues), false).update().close();
        log = Files.readFileToString(logFile, Charsets.UTF_8);
        Assertions.assertThat(countAttemptRecords(log))
                .as("the attempt that finishes the job adds exactly one more")
                .isEqualTo(2);
        Assertions.assertThat(log).as("with the committed outcome recorded").contains("OUTCOME=COMMITTED");

        //The "nothing was added" case, stated where it is deterministic: an attempt that stages nothing must leave no
        //record and no index generation. (An updater pass over a store that looks complete is NOT that case - it re-
        //derives the '_incomplete_' tail and legitimately publishes what it lands - which is why this is asserted on
        //the transaction, the unit the record belongs to.)
        final int generationsBefore = VersionedTimeSeriesMemoryFileLookupTable.listIndexGenerations(memoryDir).length;
        lookup.newUpdateTransaction(false).close();
        log = Files.readFileToString(logFile, Charsets.UTF_8);
        Assertions.assertThat(countAttemptRecords(log))
                .as("an attempt that staged nothing writes no log record")
                .isEqualTo(2);
        Assertions.assertThat(VersionedTimeSeriesMemoryFileLookupTable.listIndexGenerations(memoryDir).length)
                .as("and no index generation either")
                .isEqualTo(generationsBefore);
    }

    /**
     * Two attempts that each die before their first boundary commit: the first publishes at close and leaves a COMPLETE
     * last file behind, so the second CONTINUES that store and its own close-publish APPENDS a generation instead of
     * rewriting the chain. The re-stream of the whole chain stays where it belongs - after a pass that finished and
     * published its short tail as a standalone '_incomplete_' shard, which the next attempt has to redo - and an
     * aborted pass never pays for it.
     */
    @Test
    public void testTwoAbortedAttemptsAppendInsteadOfRewritingTheChain() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("abortAppendsChain");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        //fewer segments than the commit interval, so no boundary publish happens: everything lands at close
        final int landedPerAbort = 2 * BATCH_FLUSH_INTERVAL;
        final List<FDate> allValues = newValues(1, 4 * landedPerAbort);

        runAbortingAttempt(table, lookup, allValues, landedPerAbort);
        final File memoryDir = lookup.getDirectoryHashKeyVersionMemory().getDirectoryHashKeyVersionDataShared();
        Assertions.assertThat(VersionedTimeSeriesMemoryFileLookupTable.listIndexGenerations(memoryDir))
                .as("the first abort publishes what it finished")
                .hasSize(1);

        runAbortingAttempt(table, lookup, allValues, 2 * landedPerAbort);
        final MemoryFileMetadata metadata = lookup.getMemoryFileLookupTable().getMetadata();
        try (ICloseableProperties properties = metadata.getProperties()) {
            Assertions.assertThat(metadata.getChainBase(properties))
                    .as("the second abort's close-publish continues the chain, it does not re-stream it")
                    .isZero();
        }
        Assertions.assertThat(VersionedTimeSeriesMemoryFileLookupTable.listIndexGenerations(memoryDir))
                .as("one generation per aborted attempt")
                .hasSize(2);
        //the chain is the source of truth for how much of the range the two aborts landed: attempt 1 finished both
        //batches, attempt 2 the batches it closed before its source died (the one it was writing is gone). Counted
        //from the chain rather than hard-coded, because the exact batch boundary is the writer's business.
        final List<MemoryFileSummary> chain = readPublishedSummaries(lookup);
        int chainValueCount = 0;
        for (final MemoryFileSummary summary : chain) {
            chainValueCount += summary.getValueCount();
        }
        Assertions.checkTrue(chainValueCount > landedPerAbort && chainValueCount <= 2 * landedPerAbort,
                "the second abort must land at least one more finished batch, got: " + chainValueCount);
        Assertions.assertThat(readValues(table, KEY))
                .isEqualTo(new ArrayList<>(allValues.subList(0, chainValueCount)));
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "two aborted attempts must leave a readable store: " + chain);
    }

    /**
     * One updater pass whose source dies after {@code landed} values - the shape a timing-out feed leaves behind.
     */
    private void runAbortingAttempt(final ATimeSeriesDB<String, FDate> table,
            final TimeSeriesLookupStorageCache<String, FDate> lookup, final List<FDate> allValues, final int landed)
            throws Exception {
        boolean aborted = false;
        try {
            newUpdater(table, KEY, abortingSource(allValues, landed), false).update().close();
        } catch (final IncompleteUpdateRetryableException e) {
            aborted = true;
        }
        Assertions.checkTrue(aborted, "the pass has to die mid-way for this test to mean anything");
    }

    private static int countAttemptRecords(final String log) {
        int count = 0;
        for (int i = log.indexOf("\nTRANSACTION\n"); i >= 0; i = log.indexOf("\nTRANSACTION\n", i + 1)) {
            count++;
        }
        return count;
    }

    /**
     * A transaction closed without a final commit publishes every segment it FINISHED, not only the ones a boundary
     * commit had already landed; continuing the store with a new attempt then lands every value exactly once.
     */
    @Test
    public void testCloseWithoutCommitPublishesEveryFinishedSegment() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("boundaryCommit");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final int commitInterval = TimeSeriesProperties.UPDATE_COMMIT_SEGMENT_INTERVAL;
        final int firstBoundaryValues = commitInterval * BATCH_FLUSH_INTERVAL;
        final int landedValues = 2 * firstBoundaryValues + BATCH_FLUSH_INTERVAL;
        final int totalValues = landedValues + BATCH_FLUSH_INTERVAL;
        final List<FDate> allValues = newValues(1, totalValues);

        final TimeSeriesUpdateTransaction<FDate> transaction = lookup.newUpdateTransaction(true);
        boolean aborted = false;
        try {
            SequentialChunkedUpdateProgress.doUpdate(transaction,
                    new UpdatingInternalMethods(table, lookup, transaction.getEpochLocalDataId()),
                    transaction.getPrecedingMemoryOffset(), transaction.getMemoryOffset(),
                    transaction.getPrecedingValueCount(), skippingPublished(transaction,
                            abortingSource(allValues, landedValues)));
        } catch (final IllegalStateException e) {
            aborted = true;
        } finally {
            transaction.close();
        }
        Assertions.checkTrue(aborted, "the aborting source must propagate out of the writer");

        final List<MemoryFileSummary> published = readPublishedSummaries(lookup);
        Assertions.checkTrue(!published.isEmpty(),
                "the segments published at the boundary must survive a close without commit");
        final List<FDate> publishedValues = readValues(table, KEY);
        Assertions.checkTrue(publishedValues.size() >= firstBoundaryValues,
                "at least one boundary must be readable, got: " + publishedValues.size());
        Assertions.checkTrue(publishedValues.size() > landedValues - BATCH_FLUSH_INTERVAL
                && publishedValues.size() <= landedValues,
                "a close without commit must publish every finished segment, got " + publishedValues.size()
                        + " of " + landedValues + " landed values");
        Assertions.assertThat(publishedValues).isEqualTo(new ArrayList<>(
                allValues.subList(0, publishedValues.size())));
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "the published boundary must be a consistent store: " + published);

        //only the one batch that was still open when the pass died is lost, and the next attempt re-derives exactly
        //that range, so every value ends up in the store once
        final TimeSeriesUpdateTransaction<FDate> next = lookup.newUpdateTransaction(true);
        try {
            SequentialChunkedUpdateProgress.doUpdate(next,
                    new UpdatingInternalMethods(table, lookup, next.getEpochLocalDataId()),
                    next.getPrecedingMemoryOffset(), next.getMemoryOffset(), next.getPrecedingValueCount(),
                    skippingPublished(next, WrapperCloseableIterable.maybeWrap(allValues)));
            next.commit();
        } finally {
            next.close();
        }
        Assertions.assertThat(readValues(table, KEY)).isEqualTo(new ArrayList<>(allValues));
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "the store must stay consistent after the tail was published: " + readPublishedSummaries(lookup));
    }

    /**
     * The same guard {@link ATimeSeriesUpdater} puts in front of every attempt's source: an attempt that continues a
     * published boundary must not write the values the store already holds a second time.
     */
    private static ICloseableIterable<FDate> skippingPublished(final TimeSeriesUpdateTransaction<FDate> transaction,
            final ICloseableIterable<FDate> source) {
        final FDate resumeAfterEndTime = transaction.getResumeAfterEndTime();
        if (resumeAfterEndTime == null) {
            return source;
        }
        return new ASkippingIterable<FDate>(source) {
            @Override
            protected boolean skip(final FDate element) {
                return !element.isAfterNotNullSafe(resumeAfterEndTime);
            }
        };
    }

    /**
     * A source that hands out {@code landed} values and then throws - the way a stream that times out or gets
     * cancelled kills the pass. It paces itself like a real feed (the parallel writer's producer would otherwise race
     * far ahead of the writer loop and the pass would die before it ever wrote a segment): values are handed out at
     * about a millisecond each.
     * <p>
     * {@code publishedBoundaryWitness} is the frontier list of an {@link ITimeSeriesBoundaryCommitListener} attached to
     * the same attempt: when it is non-null the source waits for a boundary to actually become PUBLISHED before it
     * dies. The clock cannot express that - the parallel writer's producer and writer loop race, and on this box three
     * of three isolated runs of the parallel variant ended the pass before it had finished a single data file (so the
     * scenario degenerated into "nothing was ever published", measured at lane tip c5ff948e without this change).
     * Waiting on the publish is what makes the resume case mean what it says.
     */
    private static ICloseableIterable<FDate> abortingSource(final List<FDate> values, final int landed,
            final List<FDate> publishedBoundaryWitness) {
        return new ICloseableIterable<FDate>() {
            @Override
            public ICloseableIterator<FDate> iterator() {
                return new ICloseableIterator<FDate>() {

                    private int index = 0;

                    @Override
                    public boolean hasNext() {
                        return true;
                    }

                    @Override
                    public FDate next() {
                        if (index >= landed) {
                            awaitPublishedBoundary(publishedBoundaryWitness);
                            throw new IllegalStateException(
                                    "abort mid-pass after " + landed + " fetched values");
                        }
                        Duration.ONE_MILLISECOND.sleepNoInterrupt();
                        return values.get(index++);
                    }

                    @Override
                    public void close() {}
                };
            }
        };
    }

    private static ICloseableIterable<FDate> abortingSource(final List<FDate> values, final int landed) {
        return abortingSource(values, landed, null);
    }

    private static void awaitPublishedBoundary(final List<FDate> publishedBoundaryWitness) {
        if (publishedBoundaryWitness == null) {
            return;
        }
        //one millisecond per poll, so the bound below is a minute of wall time
        for (int poll = 0; publishedBoundaryWitness.isEmpty(); poll++) {
            if (poll > 60_000) {
                throw new IllegalStateException(
                        "no segment boundary became published within a minute - the writer never finished a file");
            }
            Duration.ONE_MILLISECOND.sleepNoInterrupt();
        }
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
     * An update attempt that states the epoch local data id of its transaction explicitly, the way
     * {@link ATimeSeriesUpdater} does it through its update transaction.
     */
    @NotThreadSafe
    private static final class UpdatingInternalMethods
            implements ITimeSeriesUpdaterInternalMethods<String, FDate> {

        private final ATimeSeriesDB<String, FDate> table;
        private final TimeSeriesLookupStorageCache<String, FDate> lookupTable;
        private final String epochLocalDataId;

        private UpdatingInternalMethods(final ATimeSeriesDB<String, FDate> table,
                final TimeSeriesLookupStorageCache<String, FDate> lookupTable, final String epochLocalDataId) {
            this.table = table;
            this.lookupTable = lookupTable;
            this.epochLocalDataId = epochLocalDataId;
        }

        @Override
        public ISerde<FDate> getValueSerde() {
            return table.getValueSerde();
        }

        @Override
        public TimeSeriesLookupStorageCache<String, FDate> getLookupTable() {
            return lookupTable;
        }

        @Override
        public ITimeSeriesDB<String, FDate> getTable() {
            return table;
        }

        @Override
        public FDate extractStartTime(final FDate element) {
            return element;
        }

        @Override
        public FDate extractEndTime(final FDate element) {
            return element;
        }

        @Override
        public void onElement(final ITimeSeriesUpdateProgress updateProgress, final long pendingCount) {}

        @Override
        public void onFlush(final ITimeSeriesUpdateProgress updateProgress, final long flushIndex) {}

        @Override
        public String getKey() {
            return KEY;
        }

        @Override
        public boolean shouldRedoLastFile() {
            //the default of an incremental updater: its last batch lands in an incomplete file that the next attempt
            //redoes
            return true;
        }

        @Override
        public String getEpochLocalDataId() {
            return epochLocalDataId;
        }
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
            final ICloseableIterable<? extends FDate> source, final boolean parallel) {
        return newUpdater(table, key, source, parallel, null);
    }

    /**
     * @param updateFromSeen when non-null, receives the start time this attempt was told to continue from - the
     *            store's published frontier, which is what a resumed attempt must not be asked to re-supply
     */
    private ATimeSeriesUpdater<String, FDate> newUpdater(final ATimeSeriesDB<String, FDate> table, final String key,
            final ICloseableIterable<? extends FDate> source, final boolean parallel,
            final AtomicReference<FDate> updateFromSeen) {
        return newUpdater(table, key, source, parallel, updateFromSeen, null);
    }

    /**
     * @param boundaryFrontiers when non-null, this attempt attaches a boundary-commit listener that records every
     *            published frontier - the witness an aborting source waits on instead of a sleep budget
     */
    private ATimeSeriesUpdater<String, FDate> newUpdater(final ATimeSeriesDB<String, FDate> table, final String key,
            final ICloseableIterable<? extends FDate> source, final boolean parallel,
            final AtomicReference<FDate> updateFromSeen, final List<FDate> boundaryFrontiers) {
        return new ATimeSeriesUpdater<String, FDate>(key, table) {

            @Override
            protected ICloseableIterable<? extends FDate> getSource(final FDate updateFrom) {
                if (updateFromSeen != null) {
                    updateFromSeen.set(updateFrom);
                }
                return source;
            }

            @Override
            protected ITimeSeriesBoundaryCommitListener newBoundaryCommitListener() {
                if (boundaryFrontiers == null) {
                    return null;
                }
                return new ITimeSeriesBoundaryCommitListener() {

                    @Override
                    public void onBoundaryCommitPublished(final FDate publishedEndTime) {
                        boundaryFrontiers.add(publishedEndTime);
                    }
                };
            }

            @Override
            protected boolean shouldWriteInParallel() {
                return parallel;
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
