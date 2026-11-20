package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFiles;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.AMemoryFileSummarySerializingCollection;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.MemoryFileMetadata;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.VersionedTimeSeriesMemoryFileLookupTable;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdateProgress;
import de.invesdwin.context.system.properties.ICloseableProperties;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLockRegistry;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.string.Charsets;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.Longs;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.date.millis.FDateMillis;

/**
 * What a killed or failed update attempt leaves in the store, and what recovery must do about it when a later process
 * opens the same directory again. The shape comes from a measured incident, not a theory: a surefire fork on this box
 * was cap-killed mid-pass and left attempt garbage (7 zero-byte 'memory_&lt;attemptId&gt;.data' files plus ~21-37
 * 'memory_&lt;attemptId&gt;_incomplete_&lt;offset&gt;_&lt;timestamp&gt;.data' shards) in a live cache directory; the
 * next run inherited it, produced 'Max Retries exhausted' retry-later aborts, and parked with every thread in a join
 * chain until the store was cleared. Four scenarios, each its own store directory:
 * <ol>
 * <li>foreign garbage (a different, already-dead attempt's files) must not invalidate an otherwise healthy chain - no
 * false wipe;</li>
 * <li>foreign garbage must still be swept by a later reclaim pass, and that pass must not prune anything live - even
 * when the key's byte attestation sits one generation ahead of the chain actually on disk, which is the crash window
 * {@link TimeSeriesAttestedGenerationCrashWindowTest} pins for {@link TimeSeriesLookupStorageCache#isEmptyOrInconsistent
 * ()} but which the reclaim's own high-water reliability gate did not yet know about;</li>
 * <li>a chain that really does name a 0-byte payload is damage, not garbage, and must still be reported inconsistent -
 * the sweep's refusal to touch foreign garbage must never become a licence to serve a chain whose own named file lost
 * its bytes;</li>
 * <li>a fresh update attempt over a store whose 'update.progress' marker describes a long-dead attempt (its heartbeat
 * claim older than {@link HeartbeatFileChannelLockRegistry#HEARTBEAT_TIMEOUT}, its claimed frontier ahead of what the
 * store actually holds) must still run to completion and advance the frontier, rather than refuse or loop.</li>
 * </ol>
 * The store directory is created per test in setUp and deleted in tearDown, so no cell depends on another cell's
 * leftovers and nothing here touches a real deployment's store.
 */
@NotThreadSafe
public class TimeSeriesAttemptGarbageRecoveryTest extends ATest {

    private static final int BATCH_FLUSH_INTERVAL = 4;
    private static final String KEY = "attemptGarbageKey";

    private File baseDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY,
                TimeSeriesAttemptGarbageRecoveryTest.class.getSimpleName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Files.deleteQuietly(baseDirectory);
    }

    /**
     * Scenario 1: a foreign dead attempt's 0-byte data file and its '_incomplete_' shard, planted into a directory
     * holding an otherwise healthy chain, must not make the store report itself inconsistent. This is the state a
     * cap-killed fork leaves for its successor to inherit.
     */
    @Test
    public void testForeignAttemptGarbageDoesNotInvalidateTheChain() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("foreignGarbageNoWipe");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final List<FDate> first = newValues(1, BATCH_FLUSH_INTERVAL + 2);
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(first), false).update().close();

        final List<MemoryFileSummary> published = readPublishedSummaries(lookup);
        Assertions.assertThat(published).as("a coherent chain before the plant").hasSize(2);
        assertPublishedBytesMatchNamedFiles(lookup, published);
        final List<File> namedBefore = namedFilesOf(published);
        final List<Long> lengthsBefore = new ArrayList<>();
        for (final File named : namedBefore) {
            lengthsBefore.add(Long.valueOf(named.length()));
        }

        final String foreignAttempt = attemptIdOf(FDateMillis.nowMillis() - TimeUnit.HOURS.toMillis(4), "orphan0001");
        awaitNoQueuedEpochReclaim();
        plantForeignAttemptArtifacts(lookup, foreignAttempt);

        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "a foreign attempt's garbage must not make the store report itself inconsistent");
        Assertions.assertThat(readValues(table, KEY)).as("every published value must still read back")
                .isEqualTo(new ArrayList<>(first));
        for (int i = 0; i < namedBefore.size(); i++) {
            final File named = namedBefore.get(i);
            Assertions.checkTrue(named.exists(), "the chain's own payload file must not be touched: " + named);
            Assertions.assertThat(named.length()).as("and must keep its published bytes: " + named)
                    .isEqualTo(lengthsBefore.get(i).longValue());
        }
    }

    /**
     * Scenario 2: the same foreign garbage, in a store whose byte attestation names a generation that never landed (the
     * crash window between the properties write and the generation rename, pinned for the read path at
     * TimeSeriesAttestedGenerationCrashWindowTest). The reclaim pass must still take the foreign garbage - the dead
     * attempt's files are not named by anything and not owned by a live heartbeat - without pruning a single
     * generation or invalidating the key. Before this class' own fix, the high-water gate in
     * {@link TimeSeriesEpochReclaim} treated this benign crash-window state as damage and refused the whole sweep, so
     * the garbage survived every pass indefinitely.
     */
    @Test
    public void testForeignAttemptGarbageIsReclaimedOverACrashWindowAttestationWithoutPruningAnythingLive()
            throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("foreignGarbageCrashWindowReclaim");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final List<FDate> first = newValues(1, BATCH_FLUSH_INTERVAL + 2);
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(first), false).update().close();

        final List<MemoryFileSummary> published = readPublishedSummaries(lookup);
        Assertions.assertThat(published).as("a coherent chain before the crash window").hasSize(2);
        final int landedGeneration = lookup.getMemoryFileLookupTable().getCurrentIndexNumber();
        final long highWaterOfLandedChain = readExpectedMemoryFileSize(lookup);

        //the crash window: the attestation moves to generation landedGeneration+1, the generation file never lands.
        publishOneMoreSummary(lookup, published);
        final File interruptedGeneration = new File(memoryDirectoryOf(lookup),
                AMemoryFileSummarySerializingCollection.newIndexGenerationFileName(landedGeneration + 1));
        Assertions.checkTrue(interruptedGeneration.exists(),
                "the fixture publishes the generation the kill is supposed to have lost");
        Assertions.checkTrue(interruptedGeneration.delete(), "removing it must work: " + interruptedGeneration);
        lookup.clearCaches();

        final long attestedHighWater = readExpectedMemoryFileSize(lookup);
        Assertions.checkTrue(attestedHighWater > highWaterOfLandedChain,
                "the attestation must be ahead of the chain that landed: " + attestedHighWater + " vs "
                        + highWaterOfLandedChain);
        Assertions.assertThat(lookup.getMemoryFileLookupTable().getCurrentIndexNumber())
                .as("the newest generation present is still the one the previous pass landed")
                .isEqualTo(landedGeneration);
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "an attestation of a generation that never landed must not report an intact key as damaged");

        final String foreignAttempt = attemptIdOf(FDateMillis.nowMillis() - TimeUnit.HOURS.toMillis(4), "orphan0002");
        awaitNoQueuedEpochReclaim();
        plantForeignAttemptArtifacts(lookup, foreignAttempt);
        Assertions.checkFalse(findAttemptDataFiles(lookup, foreignAttempt).isEmpty(),
                "planted garbage must exist before the sweep");

        final File dataDirectory = memoryDirectoryOf(lookup);
        final List<MemoryFileSummary> chainBefore = readPublishedSummaries(lookup);
        final List<String> namedBefore = namesOfPublishedChain(chainBefore);

        TimeSeriesEpochReclaim.reclaimObsoleteEpochs(lookup);

        Assertions.checkTrue(findAttemptDataFiles(lookup, foreignAttempt).isEmpty(),
                "the dead attempt's garbage must be swept even though the attestation sits ahead of the chain");
        Assertions.assertThat(VersionedTimeSeriesMemoryFileLookupTable.listIndexGenerations(dataDirectory).length)
                .as("the sweep must not have pruned a live generation").isEqualTo(landedGeneration + 1);
        for (final String named : namedBefore) {
            Assertions.assertThat(new File(dataDirectory, named))
                    .as("payload the live chain names must survive the sweep").exists();
        }
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "the store must still report itself consistent after the sweep");
        Assertions.assertThat(readValues(table, KEY)).as("the published chain must read unchanged after the sweep")
                .isEqualTo(new ArrayList<>(first));
    }

    /**
     * Scenario 3: a 0-byte payload that the chain itself names is damage, not garbage. The store's own length check
     * must still report it inconsistent, and the reset path must leave a state a fresh attempt can rebuild from source.
     */
    @Test
    public void testAChainNamingAZeroBytePayloadIsStillReportedInconsistent() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("chainNamedZeroByte");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final List<FDate> first = newValues(1, BATCH_FLUSH_INTERVAL + 2);
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(first), false).update().close();

        final List<MemoryFileSummary> published = readPublishedSummaries(lookup);
        Assertions.assertThat(published).hasSize(2);
        final MemoryFileSummary plainFileSummary = published.get(0);
        final File namedFile = new File(plainFileSummary.getMemoryResourceUri());
        Assertions.checkFalse(MemoryFiles.isIncompleteMemoryFile(namedFile.getName()),
                "the scenario needs a plain, non-'_incomplete_' payload file named by the chain: " + namedFile);
        Assertions.checkTrue(namedFile.length() > 0L, "the named file must have had bytes to lose: " + namedFile);

        //java.nio is used deliberately: de.invesdwin.util.lang.Files writes no byte[].
        java.nio.file.Files.write(namedFile.toPath(), new byte[0]);
        lookup.clearCaches();

        Assertions.checkTrue(lookup.isEmptyOrInconsistent(),
                "a chain that names a 0-byte payload must still be reported inconsistent, not served shorter");

        //deleteRange increments the version directory and clearCaches, so the cache object this test held from before
        //the reset is not the one the rebuilt store uses - the check below goes through the table, which fetches the
        //current cache afresh, exactly as a real post-reset consumer would.
        table.deleteRange(KEY);
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(first), false).update().close();
        Assertions.assertThat(readValues(table, KEY))
                .as("after the reset the key must rebuild from source").isEqualTo(new ArrayList<>(first));
        Assertions.checkFalse(table.isEmptyOrInconsistent(KEY),
                "the rebuilt store must report itself consistent again");
    }

    /**
     * Scenario 4: a fresh update attempt over a store whose 'update.progress' marker describes a long-dead attempt -
     * heartbeat claim older than HEARTBEAT_TIMEOUT, claimed frontier ahead of what the store actually holds - must run
     * to completion and advance the frontier, not refuse or loop. This is the shape a cap-killed fork leaves its
     * successor to inherit; the sibling scenarios pin that its FILES are swept, this one pins that its MARKER does not
     * block the next attempt.
     */
    @Test
    public void testUpdateOverAStoreWithADeadAttemptsProgressMarkerCompletes() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("deadAttemptProgressMarker");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final List<FDate> first = newValues(1, BATCH_FLUSH_INTERVAL);
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(first), false).update().close();
        final FDate storeFrontier = lookup.getLastValueEndTime();
        Assertions.checkNotNull(storeFrontier, "the first pass must have left a readable frontier");

        awaitNoQueuedEpochReclaim();

        //what a killed attempt's successor finds on disk: its marker, still claiming to be the running attempt, and a
        //heartbeat claim whose mtime has gone stale.
        final File progressFile = lookup.getUpdateProgressFile();
        final long threeHoursMillis = TimeUnit.HOURS.toMillis(3);
        TimeSeriesUpdaterProgress.writeUpdateProgress(progressFile,
                new FDate(FDateMillis.nowMillis() - threeHoursMillis), 0, 0, storeFrontier,
                storeFrontier.addDays(30));
        plantStaleLockClaim(progressFile, TimeUnit.MINUTES.toMillis(10));

        lookup.clearCaches();

        //a fresh attempt, asked for a far larger range than the stale marker's frontier, must just run: no refusal,
        //no retry-later loop, and it lands everything the source offers, not what the stale marker claims.
        final List<FDate> full = newValues(1, BATCH_FLUSH_INTERVAL * 3);
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(full), false).update().close();

        Assertions.assertThat(readValues(table, KEY)).as("the fresh attempt must advance the frontier to the full source")
                .isEqualTo(new ArrayList<>(full));
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "the store must report itself consistent after the fresh attempt");
        //A clean close moves 'update.progress' to 'update.finished' (TimeSeriesUpdaterResult.close) - which is what a
        //successful restart leaves: the dead attempt's marker is gone, not merely overwritten, so a third restart
        //inheriting this store has nothing stale left to find.
        Assertions.assertThat(TimeSeriesUpdaterProgress.readUpdateProgress(progressFile))
                .as("the fresh attempt's clean close must have removed the dead attempt's marker").isNull();
        Assertions.checkTrue(lookup.getUpdateFinishedFile().exists(),
                "and recorded its own completion where a later reader looks: " + lookup.getUpdateFinishedFile());
    }

    // --- helpers ---

    /**
     * A foreign dead attempt's own artifacts: a 0-byte published-shape data file and a byte-carrying '_incomplete_'
     * shard of its own, named by an id no published generation of this store references.
     */
    private static void plantForeignAttemptArtifacts(final TimeSeriesLookupStorageCache<String, FDate> lookup,
            final String attemptId) throws Exception {
        final File dataDirectory = memoryDirectoryOf(lookup);
        //java.nio is used deliberately: de.invesdwin.util.lang.Files writes no byte[], and a 0-byte file is the point.
        java.nio.file.Files.write(new File(dataDirectory,
                MemoryFiles.EPOCH_LOCAL_DATA_FILE_PREFIX + attemptId + ".data").toPath(), new byte[0]);
        java.nio.file.Files.write(new File(dataDirectory,
                MemoryFiles.EPOCH_LOCAL_DATA_FILE_PREFIX + attemptId
                        + ".data_incomplete_0_planted.data").toPath(),
                "planted".getBytes(Charsets.UTF_8));
    }

    /**
     * The '.lock' claim of a progress marker, owned by this very JVM (the same owner string the house marker writer
     * always uses) but with an mtime pushed far past HEARTBEAT_TIMEOUT - which is what a process that died hours ago
     * and whose lock-holding channel is long gone leaves behind.
     */
    private static void plantStaleLockClaim(final File progressFile, final long staleMillis) throws Exception {
        final File lockFile = new File(progressFile.getAbsolutePath() + ".lock");
        Files.writeStringToFile(lockFile, HeartbeatFileChannelLockRegistry.HEARTBEAT_OWNER,
                Charsets.defaultCharset());
        Assertions.checkTrue(lockFile.setLastModified(FDateMillis.nowMillis() - staleMillis),
                "lock claim must be staleable: " + lockFile);
    }

    private static File memoryDirectoryOf(final TimeSeriesLookupStorageCache<String, FDate> lookup) {
        return lookup.getDirectoryHashKeyVersionMemory().getDirectoryHashKeyVersionDataShared();
    }

    /**
     * Appends one summary to the published chain through the lookup table, which lands a new generation and moves the
     * attestation with it - adapted from TimeSeriesAttestedGenerationCrashWindowTest, the crash window it reproduces is
     * the same one the reclaim's high-water gate must tolerate.
     */
    private static void publishOneMoreSummary(final TimeSeriesLookupStorageCache<String, FDate> lookup,
            final List<MemoryFileSummary> published) {
        final MemoryFileSummary last = published.get(published.size() - 1);
        final FDate laterValue = FDateBuilder.newDate(2020, 1, 1).addDays(BATCH_FLUSH_INTERVAL + 3);
        final long precedingMemoryOffset = last.getPrecedingMemoryOffset() + last.getMemoryOffset()
                + last.getMemoryLength();
        final long precedingValueCount = last.getPrecedingValueCount() + last.getValueCount();
        final MemoryFileSummary extra = new MemoryFileSummary(laterValue, FDateSerde.GET, laterValue, laterValue,
                precedingValueCount, 1, "memory_foreignattemptcrash.data", precedingMemoryOffset, 0L, 120L);
        lookup.getMemoryFileLookupTable().put(new SingleSummaryIterator(extra));
    }

    private static long extentOf(final MemoryFileSummary summary) {
        return summary.getPrecedingMemoryOffset() + summary.getMemoryOffset() + summary.getMemoryLength();
    }

    private static long readExpectedMemoryFileSize(final TimeSeriesLookupStorageCache<String, FDate> lookup) {
        final MemoryFileMetadata metadata = lookup.getMemoryFileLookupTable().getMetadata();
        try (ICloseableProperties properties = metadata.getProperties()) {
            return metadata.getExpectedMemoryFileSize(properties);
        }
    }

    private static List<String> namesOfPublishedChain(final List<MemoryFileSummary> published) {
        final List<String> names = new ArrayList<>();
        for (final MemoryFileSummary summary : published) {
            final String name = new File(summary.getMemoryResourceUri()).getName();
            if (!names.contains(name)) {
                names.add(name);
            }
        }
        return names;
    }

    private static List<File> namedFilesOf(final List<MemoryFileSummary> published) {
        final List<File> namedFiles = new ArrayList<>();
        for (final MemoryFileSummary summary : published) {
            final File named = new File(summary.getMemoryResourceUri());
            if (!namedFiles.contains(named)) {
                namedFiles.add(named);
            }
        }
        return namedFiles;
    }

    /**
     * Read off the store the way its own length check reads it: the published chain is contiguous and every data file
     * a published generation names carries exactly the bytes the summaries naming it claim - copied from
     * TimeSeriesPublishedFileLengthInvariantTest, which pins the same invariant for the abort path.
     */
    private static void assertPublishedBytesMatchNamedFiles(final TimeSeriesLookupStorageCache<String, FDate> lookup,
            final List<MemoryFileSummary> published) {
        Assertions.checkTrue(!published.isEmpty(), "the store must have published something");
        long totalPublishedBytes = 0L;
        long highestClaimedByte = 0L;
        for (final MemoryFileSummary summary : published) {
            Assertions.assertThat(summary.getMemoryLength()).as("published length of " + summary).isGreaterThan(0L);
            totalPublishedBytes += summary.getMemoryLength();
            highestClaimedByte = Longs.max(highestClaimedByte, extentOf(summary));
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

    /**
     * The attempt local data files of one attempt id: exact 'memory_&lt;attemptId&gt;' prefix match, which covers the
     * '.data', '.data.&lt;offset&gt;' and the '_incomplete_' variants of that attempt and can never hit another
     * attempt's files.
     */
    private static List<File> findAttemptDataFiles(final TimeSeriesLookupStorageCache<String, FDate> lookup,
            final String attemptId) {
        final File dataDirectory = memoryDirectoryOf(lookup);
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

    /**
     * An attempt id whose embedded millis are given - the reclaim reads the age straight out of the id.
     */
    private static String attemptIdOf(final long createdMillis, final String token) {
        return createdMillis + "-" + token;
    }

    /**
     * Settles the sweep that the test's own successful attempts submitted to the version-cleanup thread. Planting
     * state while one of those is still queued races it, so every scenario that plants starts from an empty queue.
     */
    private static void awaitNoQueuedEpochReclaim() {
        TimeSeriesEpochReclaim.runPendingObsoleteEpochsReclaims();
        Assertions.assertThat(TimeSeriesEpochReclaim.getPendingObsoleteEpochsReclaimCount()).isZero();
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

    private static final class SingleSummaryIterator implements ICloseableIterator<MemoryFileSummary> {

        private final MemoryFileSummary summary;
        private boolean served;

        private SingleSummaryIterator(final MemoryFileSummary summary) {
            this.summary = summary;
        }

        @Override
        public boolean hasNext() {
            return !served;
        }

        @Override
        public MemoryFileSummary next() {
            if (served) {
                throw FastNoSuchElementException.getInstance("end reached");
            }
            served = true;
            return summary;
        }

        @Override
        public void close() {
            //nothing to release
        }
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
