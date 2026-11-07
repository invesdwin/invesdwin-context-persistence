package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFiles;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdateProgress;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.Longs;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FDates;

/**
 * The publish unit of the resumable rails is the segment boundary, and the boundary commit rotates the writer onto a
 * fresh data file (publish-then-yield requires it: a published file must never carry trailing bytes of a segment that
 * was not published). This class pins the byte-level contract that contract implies, read off the store itself:
 * <ul>
 * <li>a data file a published index generation names carries exactly the bytes that generation's summaries claim for
 * it - not more (unpublished tail), not less (a committed final file with no content);</li>
 * <li>after an abort mid the next file, the files of the dead attempt that survive are exactly the referenced ones
 * (everything else was reclaimable garbage and went with the abort path);</li>
 * <li>an attempt whose whole pass lands in the standalone '_incomplete_' file (a source that ends mid batch) publishes
 * summaries naming THAT file with its own bytes; the attempt's plain-named data file is left as a 0-byte unreferenced
 * artifact, the store still reports itself consistent, and the reclaim sweep takes the orphan while keeping the named
 * file - so the byte-carrying incomplete file is never mistaken for garbage.</li>
 * </ul>
 * The signature is the one a live store showed after the resumable rails night (three version directories, current one
 * holding a 0-byte 'memory_&lt;attemptId&gt;.data' beside a '_incomplete_' file carrying the published bytes): the
 * invariant holds there, and this test keeps it holding.
 */
@NotThreadSafe
public class TimeSeriesPublishedFileLengthInvariantTest extends ATest {

    private static final int BATCH_FLUSH_INTERVAL = 4;
    private static final String KEY = "byteInvariantKey";

    private File baseDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY,
                TimeSeriesPublishedFileLengthInvariantTest.class.getSimpleName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Files.deleteQuietly(baseDirectory);
    }

    /**
     * Boundary commits publish, the attempt then dies mid the next file: every file a published generation names
     * carries exactly its published bytes, the readable high-water is the last published boundary, and the dead
     * attempt leaves no unreferenced file behind.
     */
    @Test
    public void testPublishedFilesCarryExactlyTheirPublishedBytesAfterMidFileAbort() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("byteInvariantAbort");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final int commitInterval = TimeSeriesProperties.UPDATE_COMMIT_SEGMENT_INTERVAL;
        //two boundary commits worth of values, then the source dies part way into the next file
        final int boundaryValues = commitInterval * BATCH_FLUSH_INTERVAL;
        final int landedValues = 2 * boundaryValues + BATCH_FLUSH_INTERVAL / 2;
        final List<FDate> allValues = newValues(1, 3 * boundaryValues);

        final ATimeSeriesUpdater<String, FDate> aborting = newUpdater(table, KEY,
                abortingSource(allValues, landedValues), false);
        boolean aborted = false;
        try {
            aborting.update().close();
        } catch (final IncompleteUpdateRetryableException e) {
            aborted = true;
        }
        Assertions.checkTrue(aborted, "the aborted pass must propagate");

        final List<MemoryFileSummary> published = readPublishedSummaries(lookup);
        Assertions.checkTrue(published.size() >= commitInterval,
                "at least one boundary commit must have published: " + published.size());
        assertPublishedBytesMatchNamedFiles(lookup, published);
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "the store must not report itself inconsistent after an aborted attempt: " + published);

        //the readable frontier is exactly the last published boundary, not what the attempt fetched after it
        final List<FDate> publishedValues = readValues(table, KEY);
        Assertions.checkTrue(publishedValues.size() >= boundaryValues && publishedValues.size() < landedValues,
                "the readable high-water must be a published boundary: " + publishedValues.size());
        Assertions.assertThat(publishedValues).isEqualTo(new ArrayList<>(
                allValues.subList(0, publishedValues.size())));

        //abort leaves reclaimable artifacts only: what survives of the dead attempt is named by a published generation
        final List<String> referencedNames = new ArrayList<>();
        for (final MemoryFileSummary summary : published) {
            referencedNames.add(new File(summary.getMemoryResourceUri()).getName());
        }
        for (final File leftover : findAttemptDataFiles(lookup, aborting.getEpochId())) {
            Assertions.checkTrue(referencedNames.contains(leftover.getName()),
                    "the aborted attempt left an unreferenced data file behind: " + leftover);
        }
    }

    /**
     * A source that ends mid batch lands the whole pass in the standalone '_incomplete_' file: the published summary
     * names that file and its own byte length, the attempt's plain-named data file is a 0-byte unreferenced artifact,
     * and the store stays consistent and readable. This is the live signature a reader had to interpret; nothing about
     * it claims a byte that is not there.
     */
    @Test
    public void testMidBatchCompletionPublishesIncompleteFileNotTheEmptyNamedOne() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("byteInvariantIncomplete");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final List<FDate> values = newValues(1, BATCH_FLUSH_INTERVAL - 1);
        final ATimeSeriesUpdater<String, FDate> attempt = newUpdater(table, KEY,
                WrapperCloseableIterable.maybeWrap(values), false);
        attempt.update().close();
        final String attemptId = attempt.getEpochId();
        Assertions.checkNotNull(attemptId, "the attempt must have minted its data file id");

        final List<MemoryFileSummary> published = readPublishedSummaries(lookup);
        Assertions.assertThat(published).hasSize(1);
        final MemoryFileSummary summary = published.get(0);
        final File namedFile = new File(summary.getMemoryResourceUri());
        Assertions.checkTrue(MemoryFiles.isIncompleteMemoryFile(namedFile.getName()),
                "a mid batch end must publish the standalone '_incomplete_' file: " + namedFile);
        Assertions.assertThat(namedFile.length()).isEqualTo(summary.getMemoryLength());
        assertPublishedBytesMatchNamedFiles(lookup, published);
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "the store must not report itself inconsistent: " + published);
        Assertions.assertThat(readValues(table, KEY)).isEqualTo(new ArrayList<>(values));

        //the attempt's plain-named data file exists (the writer opens it when the pass starts) but carries no byte and
        //is named by no published generation: it is reclaimable garbage, while the byte-carrying incomplete file is not
        final File plainFile = new File(lookup.getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared(),
                MemoryFiles.EPOCH_LOCAL_DATA_FILE_PREFIX + attemptId + ".data");
        Assertions.checkTrue(plainFile.exists(), "the pass opened its plain-named data file: " + plainFile);
        Assertions.assertThat(plainFile.length()).as("plain-named file of a mid batch pass").isZero();

        TimeSeriesEpochReclaim.reclaimAbortedAttemptEpoch(lookup, attemptId);

        Assertions.checkFalse(plainFile.exists(), "the unreferenced 0-byte artifact must be reclaimable");
        Assertions.checkTrue(namedFile.exists(),
                "reclaim must never take a file a published generation names: " + namedFile);
        Assertions.assertThat(namedFile.length()).isEqualTo(summary.getMemoryLength());
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "the store must stay consistent through the sweep: " + published);
        Assertions.assertThat(readValues(table, KEY)).isEqualTo(new ArrayList<>(values));
    }

    /**
     * A resumed attempt rewrites the published prefix into data files of its own; the files the published generation
     * names keep their exact bytes across that redo, so a reader resolving the current generation is never handed a
     * short file.
     */
    @Test
    public void testResumeRewritesKeepNamedFilesByteExact() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("byteInvariantResume");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final List<FDate> firstPass = newValues(1, BATCH_FLUSH_INTERVAL * 3);
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(firstPass), false).update().close();
        List<MemoryFileSummary> published = readPublishedSummaries(lookup);
        assertPublishedBytesMatchNamedFiles(lookup, published);

        final List<FDate> secondPass = newValues(1, BATCH_FLUSH_INTERVAL * 3 + 2);
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(secondPass), false).update().close();
        published = readPublishedSummaries(lookup);
        assertPublishedBytesMatchNamedFiles(lookup, published);
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "the store must stay consistent across a resume: " + published);
        Assertions.assertThat(readValues(table, KEY)).isEqualTo(new ArrayList<>(secondPass));
    }

    /**
     * The invariant itself, read off the store the way its own length check reads it: the published chain is
     * contiguous (the highest byte offset any summary claims, precedingMemoryOffset + memoryOffset + memoryLength,
     * equals the total of the published byte ranges), and every data file a published generation names carries
     * exactly the bytes of the summaries that name it - a published file with an unpublished tail, or a published
     * range reaching past the file's bytes, breaks the store's own check in isEmptyOrInconsistent and would reset it.
     */
    private static void assertPublishedBytesMatchNamedFiles(final TimeSeriesLookupStorageCache<String, FDate> lookup,
            final List<MemoryFileSummary> published) {
        Assertions.checkTrue(!published.isEmpty(), "the store must have published something");
        long totalPublishedBytes = 0L;
        long highestClaimedByte = 0L;
        for (final MemoryFileSummary summary : published) {
            Assertions.assertThat(summary.getMemoryLength()).as("published length of " + summary).isGreaterThan(0L);
            totalPublishedBytes += summary.getMemoryLength();
            highestClaimedByte = Longs.max(highestClaimedByte,
                    summary.getPrecedingMemoryOffset() + summary.getMemoryOffset() + summary.getMemoryLength());
        }
        Assertions.assertThat(highestClaimedByte).as("the published byte chain must be contiguous")
                .isEqualTo(totalPublishedBytes);

        final List<File> namedFiles = new ArrayList<>();
        for (final MemoryFileSummary summary : published) {
            final File named = new File(summary.getMemoryResourceUri());
            if (!namedFiles.contains(named)) {
                namedFiles.add(named);
            }
        }
        long totalBytesOnDisk = 0L;
        for (final File namedFile : namedFiles) {
            long publishedBytesOfThisFile = 0L;
            for (final MemoryFileSummary summary : published) {
                if (namedFile.getAbsolutePath().equals(summary.getMemoryResourceUri())) {
                    publishedBytesOfThisFile += summary.getMemoryLength();
                }
            }
            Assertions.checkTrue(namedFile.exists(), "a published generation names a missing file: " + namedFile);
            Assertions.assertThat(namedFile.length()).as("bytes on disk of " + namedFile)
                    .isEqualTo(publishedBytesOfThisFile);
            totalBytesOnDisk += namedFile.length();
        }
        Assertions.assertThat(totalBytesOnDisk).as("every published byte must be on disk, and no byte beyond")
                .isEqualTo(totalPublishedBytes);
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "the store's own length check must pass: " + published);
    }

    private static ICloseableIterable<FDate> abortingSource(final List<FDate> values, final int landedValues) {
        return new ICloseableIterable<FDate>() {
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
                        if (index >= landedValues) {
                            throw new IllegalStateException(
                                    "abort mid-pass after [" + landedValues + "] landed values");
                        }
                        return values.get(index++);
                    }

                    @Override
                    public void close() {}
                };
            }
        };
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
        return new ATimeSeriesUpdater<String, FDate>(key, table) {

            @Override
            protected ICloseableIterable<? extends FDate> getSource(final FDate updateFrom) {
                return source;
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
