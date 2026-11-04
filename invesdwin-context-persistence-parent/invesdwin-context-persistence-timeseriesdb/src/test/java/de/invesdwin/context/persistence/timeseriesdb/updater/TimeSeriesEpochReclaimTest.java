package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFiles;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdateProgress;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLockRegistry;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.string.Charsets;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.date.millis.FDateMillis;

/**
 * Reclaim of the data files an update attempt leaves in the store (the attempt-local layout is what makes it
 * necessary):
 * <ul>
 * <li>the failing writer reclaims its OWN attempt files (including the '_incomplete_' variants) on the abort path -
 * provably unpinned, unpublished garbage;</li>
 * <li>the piggybacked sweep at attempt finish deletes dead attempts of that key's version directory only - an attempt
 * stays if any published index generation on disk names one of its files (M1 trap: never delete a referenced file) or
 * if the current progress marker describes a heartbeat-live attempt that began no later than that attempt was minted,
 * and never before RETAIN_OBSOLETE_EPOCHS_THRESHOLD has passed since the attempt started;</li>
 * <li>the next successful attempt for the key piggybacks the sweep, so no dedicated timer is needed.</li>
 * </ul>
 */
@NotThreadSafe
public class TimeSeriesEpochReclaimTest extends ATest {

    private static final int BATCH_FLUSH_INTERVAL = 4;
    private static final String KEY = "reclaimKey";

    private File baseDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY, TimeSeriesEpochReclaimTest.class.getSimpleName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Files.deleteQuietly(baseDirectory);
    }

    /**
     * The failing writer reclaims its own garbage in the abort path: the attempt data files (including the
     * '_incomplete_' variants) are gone when update() throws, and nothing was published.
     */
    @Test
    public void testAbortedAttemptReclaimsOwnArtifacts() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("attemptAbortReclaim");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final List<FDate> values = newValues(1, 8);
        //the source lands two batches and then throws mid-pass
        final ICloseableIterable<FDate> abortingSource = new ICloseableIterable<FDate>() {
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
                            throw new IllegalStateException("abort mid-pass after two flushed batches");
                        }
                        final FDate value = values.get(index++);
                        if (index == values.size()) {
                            //throw right after the last element is handed out, before the third flush bookkeeping
                            //completes: the attempt dies with two data files of its own on disk
                            throw new IllegalStateException("abort mid-pass after two flushed batches");
                        }
                        return value;
                    }

                    @Override
                    public void close() {}
                };
            }
        };
        final ATimeSeriesUpdater<String, FDate> updater = newUpdater(table, KEY, abortingSource, false);
        boolean aborted = false;
        try {
            updater.update().close();
        } catch (final IncompleteUpdateRetryableException e) {
            aborted = true;
        }
        Assertions.checkTrue(aborted, "the abort must propagate");
        final String attemptId = updater.getEpochId();
        Assertions.checkNotNull(attemptId, "the attempt must have minted its data file id");
        Assertions.checkTrue(findAttemptDataFiles(lookup, attemptId).isEmpty(),
                "the aborted attempt must have reclaimed its data files");
        Assertions.checkTrue(readPublishedSummaries(lookup).isEmpty(),
                "the aborted attempt must not have published anything");
    }

    /**
     * The piggybacked sweep at attempt finish: an old dead attempt's data files go, the published generation's
     * referenced files stay (M1 trap), and the attempt the current marker describes with a fresh heartbeat claim stays
     * even when it is older than the retention threshold (liveness beats age).
     */
    @Test
    public void testReclaimSweepKeepsPublishedAndLiveRemovesDeadOld() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("attemptSweep");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);

        //two successful attempts: the published generation references both attempts' landed files
        final ATimeSeriesUpdater<String, FDate> first = newUpdater(table, KEY,
                WrapperCloseableIterable.maybeWrap(newValues(1, 8)), false);
        first.update().close();
        final ATimeSeriesUpdater<String, FDate> second = newUpdater(table, KEY,
                WrapperCloseableIterable.maybeWrap(newValues(1, 12)), false);
        second.update().close();

        //plant a dead old attempt (its id predates the attempt the marker describes, nothing names it, nothing
        //refers it): swept
        final String deadAttempt = attemptIdOf(FDateMillis.nowMillis() - TimeUnit.HOURS.toMillis(4), "dead0000");
        plantAttemptArtifacts(lookup, deadAttempt);
        //plant an attempt of a live pass: the marker describes an attempt that started three hours ago (its heartbeat
        //claim is fresh) and the planted id was minted two hours ago, i.e. at or after that start - so it may still be
        //in flight and must stay although it is older than the retention threshold
        final String liveAttempt = attemptIdOf(FDateMillis.nowMillis() - TimeUnit.HOURS.toMillis(2), "live0000");
        plantAttemptArtifacts(lookup, liveAttempt);
        final File progressFile = lookup.getUpdateProgressFile();
        TimeSeriesUpdaterProgress.writeUpdateProgress(progressFile,
                new FDate(FDateMillis.nowMillis() - TimeUnit.HOURS.toMillis(3)), 0, 0, null, null);
        plantFreshLockClaim(progressFile);

        TimeSeriesEpochReclaim.reclaimObsoleteEpochs(lookup);

        Assertions.checkTrue(findAttemptDataFiles(lookup, deadAttempt).isEmpty(),
                "the dead old attempt's data files must be swept");
        for (final String attemptId : Arrays.asList(first.getEpochId(), second.getEpochId())) {
            Assertions.checkTrue(!findAttemptDataFiles(lookup, attemptId).isEmpty(),
                    "the published generation's attempt data files must stay: " + attemptId);
        }
        Assertions.checkTrue(!findAttemptDataFiles(lookup, liveAttempt).isEmpty(),
                "the live attempt's data files must never be swept");

        //the store itself stays intact and readable
        Assertions.checkFalse(lookup.isEmptyOrInconsistent());
        Assertions.assertThat(readValues(table, KEY)).isEqualTo(new ArrayList<>(newValues(1, 12)));
    }

    /**
     * The sweep needs no dedicated timer: the next successful attempt for the key piggybacks it and takes the crashed
     * writer's garbage with it.
     */
    @Test
    public void testNextSuccessfulAttemptSweepsPlantedGarbage() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("attemptPiggyback");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, 8)), false).update().close();

        final String deadAttempt = attemptIdOf(FDateMillis.nowMillis() - TimeUnit.HOURS.toMillis(2), "crash000");
        plantAttemptArtifacts(lookup, deadAttempt);
        Assertions.checkFalse(findAttemptDataFiles(lookup, deadAttempt).isEmpty(), "planted garbage must exist first");

        //the next attempt for the key finishes successfully -> its piggyback sweep takes the garbage
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, 12)), false).update().close();

        Assertions.checkTrue(findAttemptDataFiles(lookup, deadAttempt).isEmpty(),
                "the piggybacked sweep must have removed the planted data files");
        Assertions.assertThat(readValues(table, KEY)).isEqualTo(new ArrayList<>(newValues(1, 12)));
    }

    /**
     * An attempt id whose embedded millis are given - the reclaim reads the age straight out of the id.
     */
    private static String attemptIdOf(final long createdMillis, final String token) {
        return createdMillis + "-" + token;
    }

    private static void plantAttemptArtifacts(final TimeSeriesLookupStorageCache<String, FDate> lookup,
            final String attemptId) throws Exception {
        final File dataDirectory = lookup.getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        Files.writeStringToFile(new File(dataDirectory, MemoryFiles.EPOCH_LOCAL_DATA_FILE_PREFIX + attemptId + ".data"),
                "planted", Charsets.defaultCharset());
        Files.writeStringToFile(
                new File(dataDirectory, MemoryFiles.EPOCH_LOCAL_DATA_FILE_PREFIX + attemptId
                        + ".data_incomplete_0_planted.data"),
                "planted", Charsets.defaultCharset());
    }

    private static void plantFreshLockClaim(final File progressFile) throws Exception {
        final File lockFile = new File(progressFile.getAbsolutePath() + ".lock");
        Files.writeStringToFile(lockFile, HeartbeatFileChannelLockRegistry.HEARTBEAT_OWNER,
                Charsets.defaultCharset());
        Assertions.checkTrue(lockFile.setLastModified(FDateMillis.nowMillis()), "lock claim must be fresh");
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
