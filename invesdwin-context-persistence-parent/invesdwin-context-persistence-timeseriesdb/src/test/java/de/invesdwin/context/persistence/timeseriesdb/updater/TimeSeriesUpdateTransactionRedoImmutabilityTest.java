package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
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
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FDates;

/**
 * The redo-last-file path of an update transaction must never mutate or remove the data files that the currently
 * published index generation references. It may only append its own new values to files of its own attempt, so that an
 * abandoned attempt leaves nothing behind but garbage and a published generation stays readable throughout.
 */
@NotThreadSafe
public class TimeSeriesUpdateTransactionRedoImmutabilityTest extends ATest {

    private static final int BATCH_FLUSH_INTERVAL = 4;
    private static final String KEY = "redoKey";

    private File baseDirectory;
    private final List<FDate> sourceValues = new ArrayList<>();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY,
                TimeSeriesUpdateTransactionRedoImmutabilityTest.class.getSimpleName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
        sourceValues.clear();
        addValues(1, 10);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        sourceValues.clear();
        Files.deleteQuietly(baseDirectory);
    }

    @Test
    public void testRedoLastFileKeepsPublishedDataFilesImmutable() throws Exception {
        checkRedoKeepsPublishedDataFiles(false);
    }

    @Test
    public void testRedoLastFileKeepsPublishedDataFilesImmutableInParallel() throws Exception {
        checkRedoKeepsPublishedDataFiles(true);
    }

    private void checkRedoKeepsPublishedDataFiles(final boolean parallel) throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable();
        final TimeSeriesLookupStorageCache<String, FDate> lookupTable = table.getLookupTableCache(KEY);

        newUpdater(table, parallel).update().close();

        final List<MemoryFileSummary> published = readSummaries(lookupTable);
        Assertions.checkTrue(published.size() >= 2, "expected several published summaries: " + published);
        final MemoryFileSummary publishedLast = published.get(published.size() - 1);
        //the redo branch only triggers on an incomplete last file, so assert the precondition of the scenario
        Assertions.checkTrue(MemoryFiles.isIncompleteMemoryFile(publishedLast.getMemoryResourceUri()),
                "the last published summary is expected to be an incomplete file: " + publishedLast);

        final Map<File, byte[]> publishedBytes = ILockCollectionFactory.getInstance(false).newLinkedMap();
        for (final MemoryFileSummary summary : published) {
            final File dataFile = new File(summary.getMemoryResourceUri());
            publishedBytes.put(dataFile, readAllBytes(dataFile));
        }

        //grow the source and let a second attempt redo the incomplete last file of the first attempt
        addValues(11, 14);
        newUpdater(table, parallel).update().close();

        for (final Map.Entry<File, byte[]> entry : publishedBytes.entrySet()) {
            final File dataFile = entry.getKey();
            Assertions.checkTrue(dataFile.exists(),
                    "the redo removed a data file the published generation references: " + dataFile);
            Assertions.checkTrue(Arrays.equals(entry.getValue(), readAllBytes(dataFile)),
                    "the redo mutated bytes of a data file the published generation references: " + dataFile);
        }

        final List<MemoryFileSummary> republished = readSummaries(lookupTable);
        final MemoryFileSummary republishedLast = republished.get(republished.size() - 1);
        for (final MemoryFileSummary summary : republished) {
            final String dataFileName = new File(summary.getMemoryResourceUri()).getName();
            Assertions.checkTrue(MemoryFiles.isEpochLocalDataFile(dataFileName),
                    "the redo must write data files of its own: " + summary);
        }
        Assertions.checkNotEquals(republishedLast.getMemoryResourceUri(), publishedLast.getMemoryResourceUri(),
                "the redo must reference a new data file instead of rewriting the old one");
        Assertions.checkFalse(lookupTable.isEmptyOrInconsistent(),
                "the store reports itself inconsistent after the redo: " + republished);
        Assertions.assertThat(readValues(table)).isEqualTo(new ArrayList<>(sourceValues));
    }

    private void addValues(final int fromDay, final int toDay) {
        for (int day = fromDay; day <= toDay; day++) {
            sourceValues.add(FDateBuilder.newDate(2020, 1, 1).addDays(day));
        }
    }

    private static byte[] readAllBytes(final File file) throws Exception {
        return java.nio.file.Files.readAllBytes(file.toPath());
    }

    private static List<MemoryFileSummary> readSummaries(
            final TimeSeriesLookupStorageCache<String, FDate> lookupTable) {
        final List<MemoryFileSummary> summaries = new ArrayList<>();
        try (ICloseableIterator<MemoryFileSummary> it = lookupTable.getMemoryFileLookupTable().range()) {
            Lists.toListWithoutHasNext(it, summaries);
        }
        return summaries;
    }

    private static List<FDate> readValues(final ATimeSeriesDB<String, FDate> table) {
        final List<FDate> values = new ArrayList<>();
        try (ICloseableIterator<FDate> it = table.rangeValues(KEY, FDates.MIN_DATE, FDates.MAX_DATE).iterator()) {
            Lists.toListWithoutHasNext(it, values);
        }
        return values;
    }

    private ATimeSeriesDB<String, FDate> newTable() {
        return new ATimeSeriesDB<String, FDate>("redoImmutability") {

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

    private ATimeSeriesUpdater<String, FDate> newUpdater(final ATimeSeriesDB<String, FDate> table,
            final boolean parallel) {
        return new ATimeSeriesUpdater<String, FDate>(KEY, table) {

            @Override
            protected ICloseableIterable<? extends FDate> getSource(final FDate updateFrom) {
                return WrapperCloseableIterable.maybeWrap(sourceValues);
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
