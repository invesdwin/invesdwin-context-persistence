package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.updater.TimeSeriesUpdateTransaction.ConsumedEpochStatus;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdateProgress;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.error.FastNoSuchElementException;
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
 * Epoch design M3-cp reclaim + consumed re-validation:
 * <ul>
 * <li>every attempt writes its '&lt;versionDir&gt;/epoch-staging/&lt;epochId&gt;/index.snapshot' once EMPTY at attempt
 * start (right after the epoch was stamped into the progress marker), so the adoption gates can be verified before
 * the first flush, and overwrites it at every flush;</li>
 * <li>the failing writer reclaims its OWN epoch artifacts (data files + staging directory) on the abort path -
 * provably unpinned, unpublished garbage;</li>
 * <li>the piggybacked sweep at attempt finish deletes dead epochs of that key's version directory only - an epoch
 * stays if any published index generation on disk names one of its files (M1 trap: never delete a referenced file)
 * or if the current progress marker names it with a fresh heartbeat claim, and never before
 * RETAIN_OBSOLETE_EPOCHS_THRESHOLD has passed since the attempt started;</li>
 * <li>{@link TimeSeriesUpdateTransaction#validateConsumedEpochs} answers PUBLISHED / LIVE / STALE per consumed
 * '&lt;key&gt;#&lt;epochId&gt;' ledger entry without changing any behavior (the fixed-point driver's re-check).</li>
 * </ul>
 */
@NotThreadSafe
public class TimeSeriesEpochReclaimTest extends ATest {

    private static final int BATCH_FLUSH_INTERVAL = 4;
    private static final String KEY = "reclaimKey";
    private static final String OTHER_KEY = "reclaimOtherKey";

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
     * The attempt-start snapshot: while the pass has not landed a single flush yet, the staging directory and the
     * index.snapshot already exist (an empty collection), so the heartbeat + snapshot gates of adoption and consumed
     * re-validation can be satisfied before the first batch.
     */
    @Test
    public void testStartSnapshotExistsBeforeFirstFlush() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("epochStartSnapshot");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final List<FDate> values = newValues(1, 4);
        final CountDownLatch gate = new CountDownLatch(1);
        //the source blocks on its very first element: nothing is staged, no flush landed
        final ICloseableIterable<FDate> blockingSource = new ICloseableIterable<FDate>() {
            @Override
            public ICloseableIterator<FDate> iterator() {
                return new ICloseableIterator<FDate>() {

                    private final AtomicInteger pulled = new AtomicInteger();

                    @Override
                    public boolean hasNext() {
                        return pulled.get() < values.size();
                    }

                    @Override
                    public FDate next() {
                        if (pulled.incrementAndGet() == 1) {
                            try {
                                if (!gate.await(30, TimeUnit.SECONDS)) {
                                    throw new IllegalStateException("gate never opened");
                                }
                            } catch (final InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        if (pulled.get() > values.size()) {
                            throw FastNoSuchElementException.getInstance("exhausted");
                        }
                        return values.get(pulled.get() - 1);
                    }

                    @Override
                    public void close() {}
                };
            }
        };
        final ATimeSeriesUpdater<String, FDate> updater = newUpdater(table, KEY, blockingSource, false);
        final Thread attemptThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    updater.update().close();
                } catch (final IncompleteUpdateRetryableException e) {
                    throw new RuntimeException(e);
                }
            }
        }, "epochStartSnapshotAttempt");
        attemptThread.start();
        try {
            //wait until the marker carries the epoch (stamped at attempt start, before the source is opened)
            String epochId = null;
            final long deadline = FDateMillis.nowMillis() + TimeUnit.SECONDS.toMillis(30);
            while (epochId == null && FDateMillis.nowMillis() < deadline) {
                final TimeSeriesUpdaterProgress marker = TimeSeriesUpdaterProgress
                        .readUpdateProgress(lookup.getUpdateProgressFile());
                epochId = marker != null ? marker.getEpochId() : null;
                if (epochId == null) {
                    Thread.sleep(20);
                }
            }
            Assertions.checkNotNull(epochId, "the attempt must have stamped its epoch into the marker");
            final File snapshotFile = new File(TimeSeriesUpdateTransaction.getEpochStagingDirectory(lookup, epochId),
                    TimeSeriesUpdateTransaction.EPOCH_INDEX_SNAPSHOT_FILE_NAME);
            Assertions.checkTrue(snapshotFile.exists(),
                    "the start-time index.snapshot must exist before the first flush: " + snapshotFile);
            final List<MemoryFileSummary> staged = TimeSeriesUpdateTransaction.readEpochIndexSnapshot(lookup,
                    epochId);
            Assertions.checkNotNull(staged, "the start-time snapshot must be readable");
            Assertions.checkTrue(staged.isEmpty(), "no flush landed yet, the snapshot must be empty: " + staged);
        } finally {
            gate.countDown();
            attemptThread.join(TimeUnit.SECONDS.toMillis(60));
        }
        Assertions.checkFalse(attemptThread.isAlive(), "the attempt must have completed");
        //after completion the snapshot carries the landed batches and the values are published
        final List<MemoryFileSummary> stagedFinal = TimeSeriesUpdateTransaction.readEpochIndexSnapshot(lookup,
                updater.getEpochId());
        Assertions.checkNotNull(stagedFinal);
        Assertions.checkFalse(stagedFinal.isEmpty(), "flushes overwrite the start snapshot with staged content");
        Assertions.assertThat(readValues(table, KEY)).isEqualTo(new ArrayList<>(values));
    }

    /**
     * The failing writer reclaims its own epoch garbage in the abort path: both the epoch data files (including the
     * '_incomplete_' variants) and the staging directory are gone when update() throws, and nothing was published.
     */
    @Test
    public void testAbortedAttemptReclaimsOwnEpochArtifacts() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("epochAbortReclaim");
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
                            final FDate delayed = value;
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
        final String epochId = updater.getEpochId();
        Assertions.checkNotNull(epochId, "the attempt must have minted an epoch id");
        Assertions.checkFalse(
                TimeSeriesUpdateTransaction.getEpochStagingDirectory(lookup, epochId).exists(),
                "the aborted attempt must have reclaimed its staging directory");
        Assertions.checkTrue(findEpochDataFiles(lookup, epochId).isEmpty(),
                "the aborted attempt must have reclaimed its epoch data files");
        Assertions.checkTrue(readPublishedSummaries(lookup).isEmpty(),
                "the aborted attempt must not have published anything");
    }

    /**
     * The piggybacked sweep at attempt finish: an old dead epoch's staging directory and data files go, the published
     * generation's referenced files stay (M1 trap), and a heartbeat-live epoch named by the current marker stays even
     * when its id is older than the retention threshold.
     */
    @Test
    public void testReclaimSweepKeepsPublishedAndLiveRemovesDeadOld() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("epochSweep");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);

        //two successful attempts: the current published generation references both epochs' landed files
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, 8)), false).update().close();
        final String epoch1 = lastEpochId(lookup);
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, 12)), false).update().close();
        final String epoch2 = lastEpochId(lookup);

        //plant a dead old epoch (id millis older than the retention threshold, no marker names it, nothing refers it)
        final String deadEpoch = oldEpochId("dead0000");
        plantEpochArtifacts(lookup, deadEpoch);
        //plant a live epoch: its id is also older than the threshold, but the current marker names it and its heartbeat
        //claim is fresh - liveness must win over age
        final String liveEpoch = oldEpochId("live0000");
        plantEpochArtifacts(lookup, liveEpoch);
        final File progressFile = lookup.getUpdateProgressFile();
        TimeSeriesUpdaterProgress.writeUpdateProgress(progressFile, FDate.now(), 0, 0, null, null, liveEpoch, null);
        plantFreshLockClaim(progressFile);

        TimeSeriesEpochReclaim.reclaimObsoleteEpochs(lookup);

        Assertions.checkFalse(
                TimeSeriesUpdateTransaction.getEpochStagingDirectory(lookup, deadEpoch).exists(),
                "the dead old epoch's staging directory must be swept");
        Assertions.checkTrue(findEpochDataFiles(lookup, deadEpoch).isEmpty(),
                "the dead old epoch's data files must be swept");
        for (final String epoch : Arrays.asList(epoch1, epoch2)) {
            Assertions.checkTrue(!findEpochDataFiles(lookup, epoch).isEmpty(),
                    "the published generations' epoch data files must stay: " + epoch);
        }
        Assertions.checkTrue(
                TimeSeriesUpdateTransaction.getEpochStagingDirectory(lookup, liveEpoch).exists(),
                "the live epoch's staging directory must never be swept");
        Assertions.checkTrue(!findEpochDataFiles(lookup, liveEpoch).isEmpty(),
                "the live epoch's data files must never be swept");

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
        final ATimeSeriesDB<String, FDate> table = newTable("epochPiggyback");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, 8)), false).update().close();

        final String deadEpoch = oldEpochId("crash000");
        plantEpochArtifacts(lookup, deadEpoch);
        Assertions.checkFalse(findEpochDataFiles(lookup, deadEpoch).isEmpty(), "planted garbage must exist first");

        //the next attempt for the key finishes successfully -> its piggyback sweep takes the garbage
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, 12)), false).update().close();

        Assertions.checkFalse(
                TimeSeriesUpdateTransaction.getEpochStagingDirectory(lookup, deadEpoch).exists(),
                "the piggybacked sweep must have removed the planted staging directory");
        Assertions.checkTrue(findEpochDataFiles(lookup, deadEpoch).isEmpty(),
                "the piggybacked sweep must have removed the planted data files");
        Assertions.assertThat(readValues(table, KEY)).isEqualTo(new ArrayList<>(newValues(1, 12)));
    }

    /**
     * The consumed re-validation helper (for the trading fixed-point driver): an epoch whose files are named by the
     * published generation answers PUBLISHED, a heartbeat-live attempt with a snapshot answers LIVE, an epoch that is
     * neither answers STALE (and malformed entries are STALE too). Pure observation: nothing changes on disk.
     */
    @Test
    public void testValidateConsumedEpochsAnswersPublishedLiveStale() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("epochConsumedValidation");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);

        //PUBLISHED: a normal successful attempt - its epoch is named by the published generation
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, 8)), false).update().close();
        final String publishedEpoch = lastEpochId(lookup);

        //LIVE: a staged snapshot of an attempt whose marker + heartbeat claim are fresh
        final String liveEpoch = oldEpochId("liveval1");
        plantEpochArtifacts(lookup, liveEpoch);
        final File progressFile = lookup.getUpdateProgressFile();
        TimeSeriesUpdaterProgress.writeUpdateProgress(progressFile, FDate.now(), 0, 0, null, null, liveEpoch, null);
        plantFreshLockClaim(progressFile);

        //STALE: an epoch nothing names (and no such key directory at all for the second entry)
        final String staleEpoch = oldEpochId("gone0001");
        final List<String> entries = Arrays.asList(KEY + "#" + publishedEpoch, KEY + "#" + liveEpoch,
                KEY + "#" + staleEpoch, OTHER_KEY + "#" + publishedEpoch, "malformedWithoutHash");

        final Map<String, ConsumedEpochStatus> statuses = TimeSeriesUpdateTransaction
                .validateConsumedEpochs(lookup, entries);

        Assertions.checkEquals(ConsumedEpochStatus.PUBLISHED, statuses.get(KEY + "#" + publishedEpoch));
        Assertions.checkEquals(ConsumedEpochStatus.LIVE, statuses.get(KEY + "#" + liveEpoch));
        Assertions.checkEquals(ConsumedEpochStatus.STALE, statuses.get(KEY + "#" + staleEpoch));
        Assertions.checkEquals(ConsumedEpochStatus.STALE, statuses.get(OTHER_KEY + "#" + publishedEpoch));
        Assertions.checkEquals(ConsumedEpochStatus.STALE, statuses.get("malformedWithoutHash"));
        Assertions.checkEquals(entries.size(), statuses.size());
    }

    private String lastEpochId(final TimeSeriesLookupStorageCache<String, FDate> lookup) {
        final TimeSeriesUpdaterProgress marker = TimeSeriesUpdaterProgress
                .readUpdateProgress(lookup.getUpdateFinishedFile());
        if (marker == null || marker.getEpochId() == null) {
            throw new IllegalStateException("no finished marker with an epoch: " + marker);
        }
        return marker.getEpochId();
    }

    /**
     * An epoch id whose embedded millis are two hours in the past - beyond the default
     * RETAIN_OBSOLETE_EPOCHS_THRESHOLD of one hour.
     */
    private static String oldEpochId(final String token) {
        return (FDateMillis.nowMillis() - TimeUnit.HOURS.toMillis(2)) + "-" + token;
    }

    private static void plantEpochArtifacts(final TimeSeriesLookupStorageCache<String, FDate> lookup,
            final String epochId) throws Exception {
        final File stagingDirectory = TimeSeriesUpdateTransaction.getEpochStagingDirectory(lookup, epochId);
        Files.forceMkdir(stagingDirectory);
        Files.writeStringToFile(new File(stagingDirectory, TimeSeriesUpdateTransaction.EPOCH_INDEX_SNAPSHOT_FILE_NAME),
                "", Charsets.defaultCharset());
        final File dataDirectory = lookup.getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        Files.writeStringToFile(new File(dataDirectory, "memory_" + epochId + ".data"), "planted",
                Charsets.defaultCharset());
        Files.writeStringToFile(new File(dataDirectory, "memory_" + epochId + ".data_incomplete_0_planted.data"),
                "planted", Charsets.defaultCharset());
    }

    private static void plantFreshLockClaim(final File progressFile) throws Exception {
        final File lockFile = new File(progressFile.getAbsolutePath() + ".lock");
        Files.writeStringToFile(lockFile, HeartbeatFileChannelLockRegistry.HEARTBEAT_OWNER,
                Charsets.defaultCharset());
        Assertions.checkTrue(lockFile.setLastModified(FDateMillis.nowMillis()), "lock claim must be fresh");
    }

    private static List<File> findEpochDataFiles(final TimeSeriesLookupStorageCache<String, FDate> lookup,
            final String epochId) {
        final File dataDirectory = lookup.getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        final File[] files = dataDirectory.listFiles((dir, name) -> name.startsWith("memory_" + epochId));
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
