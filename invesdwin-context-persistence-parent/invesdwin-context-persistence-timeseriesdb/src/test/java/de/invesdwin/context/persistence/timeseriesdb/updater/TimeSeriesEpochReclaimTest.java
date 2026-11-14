package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannel;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummarySerde;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFiles;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.AMemoryFileSummarySerializingCollection;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.MemoryFileMetadata;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.VersionedTimeSeriesMemoryFileLookupTable;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdateProgress;
import de.invesdwin.context.system.properties.ICloseableProperties;
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
import de.invesdwin.util.lang.string.description.TextDescription;
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
     * The failing writer reclaims its own garbage in the abort path - and only its garbage: the batch it was still
     * writing when the source died is gone when update() throws, while every file it FINISHED is published content the
     * sweep has to keep.
     */
    @Test
    public void testAbortedAttemptKeepsWhatItFinishedAndReclaimsTheRest() throws Exception {
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
        //An aborted attempt publishes every file it FINISHED, so those files became chain content and the sweep must
        //keep them; the only thing this attempt owns that no published generation names is the batch it was writing
        //when the source died, and that is exactly what the abort reclaim deletes. Counting both sides is what makes
        //the deletion visible: one survivor more than the chain names means the garbage survived.
        final List<MemoryFileSummary> published = readPublishedSummaries(lookup);
        Assertions.checkTrue(!published.isEmpty(),
                "an aborted attempt must keep the segments it finished");
        final List<String> publishedNames = new ArrayList<>();
        for (final MemoryFileSummary summary : published) {
            publishedNames.add(new File(summary.getMemoryResourceUri()).getName());
        }
        final List<File> ownFiles = findAttemptDataFiles(lookup, attemptId);
        for (final File own : ownFiles) {
            Assertions.checkTrue(publishedNames.contains(own.getName()),
                    "the abort left an unreferenced data file behind: " + own);
            Assertions.checkFalse(MemoryFiles.isIncompleteMemoryFile(own.getName()),
                    "an aborted attempt must not leave '_incomplete_' litter behind: " + own);
        }
        Assertions.assertThat(ownFiles).as("only the files the published chain names may survive the abort reclaim")
                .hasSize(publishedNames.size());
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
        awaitNoQueuedEpochReclaim();
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
     * The sweep needs no dedicated timer: the next successful attempt for the key submits it and the version-cleanup
     * thread takes the crashed writer's garbage with it. Submitting instead of sweeping on the update thread is the
     * point - a sweep parses every retained index generation of the key (~865 MB measured for one live key's directory
     * on ptstaging) and must not run under the cache update lock. The drain happens on another thread, so this waits
     * for its effect instead of asserting about a thread it does not own.
     */
    @Test
    public void testNextSuccessfulAttemptSweepsPlantedGarbage() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("attemptPiggyback");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, 8)), false).update().close();

        final String deadAttempt = attemptIdOf(FDateMillis.nowMillis() - TimeUnit.HOURS.toMillis(2), "crash000");
        awaitNoQueuedEpochReclaim();
        plantAttemptArtifacts(lookup, deadAttempt);
        Assertions.checkFalse(findAttemptDataFiles(lookup, deadAttempt).isEmpty(), "planted garbage must exist first");

        //the next attempt for the key finishes successfully -> it submits the sweep to the janitor
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, 12)), false).update().close();

        for (int i = 0; i < 100 && !findAttemptDataFiles(lookup, deadAttempt).isEmpty(); i++) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        Assertions.checkTrue(findAttemptDataFiles(lookup, deadAttempt).isEmpty(),
                "the submitted sweep must have removed the planted data files");
        Assertions.assertThat(readValues(table, KEY)).isEqualTo(new ArrayList<>(newValues(1, 12)));
    }

    /**
     * Queue semantics of the submission: finished attempts of one key coalesce into one queued sweep, enqueueing does
     * not sweep on the calling thread, and the drain the janitor runs consumes the queue. Enqueued directly here, so
     * nothing asks the janitor for a drain and the assertions cannot race it (the other tests' drains have completed
     * by the time this runs, each having waited for its own garbage to disappear).
     */
    @Test
    public void testSubmissionsCoalescePerDirectoryAndTheDrainConsumesThem() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("attemptQueue");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, 8)), false).update().close();

        final String deadAttempt = attemptIdOf(FDateMillis.nowMillis() - TimeUnit.HOURS.toMillis(2), "queue000");
        awaitNoQueuedEpochReclaim();
        plantAttemptArtifacts(lookup, deadAttempt);
        Assertions.checkFalse(findAttemptDataFiles(lookup, deadAttempt).isEmpty(), "planted garbage must exist first");

        Assertions.checkTrue(TimeSeriesEpochReclaim.enqueueObsoleteEpochsReclaim(lookup),
                "the first submission of a data directory must queue");
        Assertions.checkFalse(TimeSeriesEpochReclaim.enqueueObsoleteEpochsReclaim(lookup),
                "a further submission of the same data directory must coalesce into it");
        Assertions.assertThat(TimeSeriesEpochReclaim.getPendingObsoleteEpochsReclaimCount()).isEqualTo(1);
        Assertions.checkFalse(findAttemptDataFiles(lookup, deadAttempt).isEmpty(),
                "enqueueing alone must not sweep on the calling thread");

        TimeSeriesEpochReclaim.runPendingObsoleteEpochsReclaims();

        Assertions.checkTrue(findAttemptDataFiles(lookup, deadAttempt).isEmpty(),
                "the drain must have removed the planted data files");
        Assertions.assertThat(TimeSeriesEpochReclaim.getPendingObsoleteEpochsReclaimCount()).isZero();
        Assertions.assertThat(readValues(table, KEY)).isEqualTo(new ArrayList<>(newValues(1, 8)));
    }

    /**
     * The reference set is the ONLY evidence of what the published index still names, and every payload file carries an
     * attempt id, so an unreadable newest generation must make the sweep skip the directory rather than delete the
     * files it can no longer see. Without the fail-closed rule this assertion is the data-loss case: a newest
     * generation truncated by a crash un-references the store's own most recent publish.
     */
    @Test
    public void testUnreadableNewestGenerationMakesTheSweepSkipTheDirectory() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("reclaimCorruptNewest");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, 8)), false).update().close();
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, 12)), false).update().close();

        final String deadAttempt = attemptIdOf(FDateMillis.nowMillis() - TimeUnit.HOURS.toMillis(2), "crpt0001");
        awaitNoQueuedEpochReclaim();
        plantAttemptArtifacts(lookup, deadAttempt);
        final File corrupted = corruptGeneration(lookup, true);

        TimeSeriesEpochReclaim.reclaimObsoleteEpochs(lookup);

        Assertions.checkFalse(findAttemptDataFiles(lookup, deadAttempt).isEmpty(),
                "an unreadable newest generation [" + corrupted.getName()
                        + "] must stop the sweep rather than let it delete what it cannot name");
    }

    /**
     * The tolerated tier of a delta chain is only what sits below the crash fallback: those generations are prunable
     * garbage whatever their contents hold, so an unreadable one must not stall the reclaim forever. Inside the live
     * range there is no tolerance - a delta names payload no other generation names - which is the inversion of the
     * cumulative layout's rule, pinned in testUnreadableChainMemberInsideTheLiveRangeMakesTheSweepSkipTheDirectory.
     */
    @Test
    public void testUnreadableGenerationBelowTheFallbackIsTolerated() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("reclaimCorruptOlder");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        //four overlapping passes: each rewrites, so the base ends above the generation this scenario corrupts
        for (int i = 1; i <= 4; i++) {
            newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, 4 * i)), false).update().close();
        }
        final File dataDirectory = lookup.getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        final int chainBase = readChainBase(lookup);
        Assertions.checkTrue(chainBase >= 3,
                "the scenario needs a superseded range below the crash fallback, got base: " + chainBase);

        final String deadAttempt = attemptIdOf(FDateMillis.nowMillis() - TimeUnit.HOURS.toMillis(2), "tolr0001");
        awaitNoQueuedEpochReclaim();
        plantAttemptArtifacts(lookup, deadAttempt);
        corruptGeneration(lookup, false);

        TimeSeriesEpochReclaim.reclaimObsoleteEpochs(lookup);

        Assertions.checkTrue(findAttemptDataFiles(lookup, deadAttempt).isEmpty(),
                "an unreadable generation below the fallback must not block the reclaim of un-named garbage");
        Assertions.checkTrue(generationPresent(dataDirectory, chainBase), "the base is untouched by the corruption");
        Assertions.checkTrue(generationPresent(dataDirectory, chainBase - 1), "and so is the crash fallback");
        Assertions.assertThat(readValues(table, KEY)).isEqualTo(new ArrayList<>(newValues(1, 16)));
    }

    /**
     * Inside the live range an unreadable generation is fatal, not ignorable: under a delta chain it is the only record
     * of payload the store believes it published, so a name set that missed it would delete live data. This is the
     * generation-level form of the same rule the payload sweep already follows for an unreadable newest generation.
     */
    @Test
    public void testUnreadableChainMemberInsideTheLiveRangeMakesTheSweepSkipTheDirectory() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("reclaimCorruptMiddle");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final int boundaryValues = 3 * TimeSeriesUpdateTransaction.PUBLISH_SEGMENTS_PER_GENERATION * BATCH_FLUSH_INTERVAL;
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, boundaryValues + 2)), false).update()
                .close();
        final File dataDirectory = lookup.getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        final List<File> generations = findGenerations(dataDirectory);
        Assertions.checkTrue(generations.size() >= 3,
                "the scenario needs a chain with a member between its base and its newest, got: "
                        + generations.size());

        final String deadAttempt = attemptIdOf(FDateMillis.nowMillis() - TimeUnit.HOURS.toMillis(2), "midcr001");
        awaitNoQueuedEpochReclaim();
        plantAttemptArtifacts(lookup, deadAttempt);
        //a member that is neither the base nor the newest: only the cumulative layout could afford to ignore it
        final File middle = generations.get(generations.size() / 2);
        Files.writeStringToFile(middle, "not-a-collection-anymore", Charsets.defaultCharset());

        TimeSeriesEpochReclaim.reclaimObsoleteEpochs(lookup);

        Assertions.checkFalse(findAttemptDataFiles(lookup, deadAttempt).isEmpty(),
                "an unreadable chain member [" + middle.getName() + "] must stop the sweep rather than let it delete"
                        + " payload only that generation names");
    }

    /**
     * Published bytes that no index generation names are damage, not garbage: with the index gone the sweep must refuse,
     * because "names nothing" would otherwise read as "every payload file is reclaimable".
     */
    @Test
    public void testPublishedBytesWithoutAnyIndexGenerationSurviveTheSweep() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("reclaimNoIndex");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, 8)), false).update().close();

        final String deadAttempt = attemptIdOf(FDateMillis.nowMillis() - TimeUnit.HOURS.toMillis(2), "noidx001");
        awaitNoQueuedEpochReclaim();
        plantAttemptArtifacts(lookup, deadAttempt);
        final File dataDirectory = lookup.getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        final File[] generations = dataDirectory.listFiles((dir, name) -> name.endsWith(
                "_" + AMemoryFileSummarySerializingCollection.MEMORY_INDEX_FILE_NAME));
        Assertions.checkTrue(generations != null && generations.length > 0,
                "the published attempt must have left an index generation to remove");
        for (final File generation : generations) {
            Assertions.checkTrue(generation.delete(), "index generation must be removable: " + generation);
        }

        TimeSeriesEpochReclaim.reclaimObsoleteEpochs(lookup);

        Assertions.checkFalse(findAttemptDataFiles(lookup, deadAttempt).isEmpty(),
                "a directory whose published payload no index names must be left alone, not cleared");
    }

    /**
     * Inside one attempt, every boundary-commit publish continues the chain, so each generation above the base names
     * payload no other generation names. Not one of them may be pruned: a live range with a hole in it is a chain that
     * cannot be assembled. This is the shape the store spends most of its publishes in (the worst ptstaging key carries
     * 781 generations per attempt, measured 2026-11-14 from lap-mid over ssh), and the count is pinned to what was
     * published rather than to the keep floor.
     */
    @Test
    public void testTheLiveChainRangeIsNeverPruned() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("keepLiveRange");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        //one pass long enough to publish several generations by boundary commits, which is where appends live
        final int boundaryValues = 3 * TimeSeriesUpdateTransaction.PUBLISH_SEGMENTS_PER_GENERATION * BATCH_FLUSH_INTERVAL;
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, boundaryValues + 2)), false).update()
                .close();
        final File dataDirectory = lookup.getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        final List<File> before = findGenerations(dataDirectory);
        Assertions.checkTrue(before.size() > TimeSeriesEpochReclaim.INDEX_GENERATIONS_KEEP_FLOOR,
                "the scenario needs more generations than the keep floor, got: " + before.size());
        final int chainBase = readChainBase(lookup);
        Assertions.checkTrue(chainBase == 0,
                "a continuing publish appends, so the base stays the first generation of the pass, got: " + chainBase);
        final List<FDate> published = readValues(table, KEY);
        final List<String> namedBefore = namesOfAllGenerations(lookup, dataDirectory);
        // Ageing happens last: a completed update submits this key directory to the epoch sweep, so from the moment the
        // generations pass the in-flight bound a sweep on the cleanup thread may prune them, and reading their contents
        // behind that moment races this class's own production code.
        awaitNoQueuedEpochReclaim();
        ageGenerations(dataDirectory, TimeUnit.HOURS.toMillis(2));

        TimeSeriesEpochReclaim.reclaimObsoleteEpochs(lookup);

        Assertions.assertThat(findGenerations(dataDirectory))
                .as("not one generation of the live chain may go - each names payload the others do not")
                .hasSize(before.size());
        Assertions.assertThat(readValues(table, KEY)).as("the published chain must read unchanged").isEqualTo(published);
        for (final String named : namedBefore) {
            Assertions.assertThat(new File(dataDirectory, named))
                    .as("payload that any pruned generation named must stay readable").exists();
        }
    }

    /**
     * The live pattern is not only appending: a demand that re-opens the last published file re-publishes an overlapping
     * range, and re-naming the region the chain tail already names is the one operation the index has that is not an
     * append. It rewrites the chain and the rewrite becomes the new base, which is what puts the superseded snapshots
     * BELOW the base - and those are exactly the generations the sweep may prune, without the coverage proof the
     * cumulative layout needed, because the rewrite re-emits everything they name. The highest one below the base
     * stays: it is the last intact chain should the new base ever prove unusable.
     */
    @Test
    public void testGenerationsBelowTheChainBaseArePrunedAndTheirPayloadSurvives() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("pruneBelowBase");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        for (int i = 1; i <= 5; i++) {
            newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, 4 * i)), false).update().close();
        }
        final File dataDirectory = lookup.getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        final List<File> before = findGenerations(dataDirectory);
        Assertions.checkTrue(before.size() > TimeSeriesEpochReclaim.INDEX_GENERATIONS_KEEP_FLOOR,
                "the scenario needs more generations than the keep floor, got: " + before.size());
        final int chainBase = readChainBase(lookup);
        Assertions.checkTrue(chainBase > 0,
                "a re-published range must have moved the base above the generations it replaces, got: " + chainBase);
        final List<String> namedBefore = namesOfAllGenerations(lookup, dataDirectory);
        final List<FDate> published = readValues(table, KEY);
        awaitNoQueuedEpochReclaim();
        ageGenerations(dataDirectory, TimeUnit.HOURS.toMillis(2));

        TimeSeriesEpochReclaim.reclaimObsoleteEpochs(lookup);

        final List<File> after = findGenerations(dataDirectory);
        Assertions.assertThat(after).as("the live range plus the highest superseded snapshot")
                .hasSize(before.size() - chainBase + 1);
        for (int number = chainBase; number < before.size(); number++) {
            Assertions.checkTrue(generationPresent(dataDirectory, number),
                    "the live chain range [" + chainBase + "..] must survive the sweep intact: " + number);
        }
        Assertions.checkTrue(generationPresent(dataDirectory, chainBase - 1),
                "the highest superseded generation stays as the crash fallback for a base that proves unusable");
        for (int number = 0; number < chainBase - 1; number++) {
            Assertions.checkFalse(generationPresent(dataDirectory, number),
                    "a superseded snapshot below the fallback must go: " + number);
        }
        Assertions.assertThat(readValues(table, KEY)).as("the published chain must read unchanged").isEqualTo(published);
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(), "the pruned store must still report itself consistent");
        for (final String named : namesOfAllGenerations(lookup, dataDirectory)) {
            Assertions.assertThat(new File(dataDirectory, named))
                    .as("every payload file the surviving chain names must exist").exists();
        }
        Assertions.checkTrue(namedBefore.containsAll(namesOfAllGenerations(lookup, dataDirectory)),
                "the sweep must not have introduced names the store never published");
    }

    /**
     * A generation file that was written moments ago may still be in flight: unlinking it does not make the writer
     * re-create it (the writer keeps the unlinked inode), so the publish being written into it simply vanishes.
     */
    @Test
    public void testYoungIndexGenerationsAreNotPruned() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("pruneYoung");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        for (int i = 1; i <= 5; i++) {
            newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, 4 * i)), false).update().close();
        }
        final File dataDirectory = lookup.getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        final List<File> before = findGenerations(dataDirectory);

        TimeSeriesEpochReclaim.reclaimObsoleteEpochs(lookup);

        Assertions.assertThat(findGenerations(dataDirectory))
                .as("nothing may be pruned while every generation is younger than the in-flight bound")
                .hasSize(before.size());
    }

    /**
     * A newest generation that does not parse makes the whole name set untrustworthy, so the walk returns unreliable and
     * the generation prune is skipped. The pin is on the file the walk could not read: a pass that cannot parse the newest
     * chain must not delete it, because it is the only record of the most recent publishes.
     */
    @Test
    public void testUnreadableNewestGenerationAlsoStopsTheGenerationPrune() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("pruneCorruptNewest");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        for (int i = 1; i <= 5; i++) {
            newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, 4 * i)), false).update().close();
        }
        final File dataDirectory = lookup.getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        //corrupted before aged: with an unreliable newest generation every sweep refuses, so no background pass can prune
        //between ageing and the assertion below
        final File corrupted = corruptGeneration(lookup, true);
        final String deadAttempt = attemptIdOf(FDateMillis.nowMillis() - TimeUnit.HOURS.toMillis(2), "cnat0001");
        awaitNoQueuedEpochReclaim();
        plantAttemptArtifacts(lookup, deadAttempt);
        ageGenerations(dataDirectory, TimeUnit.HOURS.toMillis(2));

        TimeSeriesEpochReclaim.reclaimObsoleteEpochs(lookup);

        //The generation prune's own refusal is pinned by what it must never do to the file it could not read: a count
        //pin here is not stable, because a pass that read the chain before it was corrupted is entitled to prune, and
        //the lease cleanup thread runs one whenever these updates complete (measured: expected 5 but was 4, the
        //difference made by that pass, not by the one under test). What no pass may do is delete the newest generation
        //it cannot parse - it is the only record of the most recent publishes.
        Assertions.assertThat(corrupted).as("a pass that cannot parse the newest generation may not delete it")
                .exists();
        Assertions.assertThat(corrupted.length()).as("and may not rewrite it").isGreaterThan(0L);
        //the reliability gate's teeth are on the attempt path: today every unreliable return happens before a generation
        //is marked prunable, so the prune's own isReliable() check is belt-and-braces. This assertion is what would go
        //red if an unreliable walk were allowed to delete.
        Assertions.assertThat(findAttemptDataFiles(lookup, deadAttempt))
                .as("an untrustworthy walk may not reclaim any attempt group either")
                .isNotEmpty();
    }

    /**
     * The tier the two reliability checks cannot see: a newest generation truncated at a record boundary still parses,
     * so it yields a valid but SHORTER chain - and the store persists the byte high-water of the chain it published, so
     * the mismatch is detectable. Pruning or reclaiming on such a chain is the data-loss case.
     */
    @Test
    public void testNewestGenerationShorterThanTheAttestedHighWaterStopsTheSweep() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("pruneTruncatedNewest");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, 8)), false).update().close();
        newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, 20)), false).update().close();
        final File dataDirectory = lookup.getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        final List<MemoryFileSummary> chain = readPublishedSummaries(lookup);
        Assertions.checkTrue(chain.size() >= 2, "the scenario needs a chain of at least two summaries, got: "
                + chain.size());

        final String deadAttempt = attemptIdOf(FDateMillis.nowMillis() - TimeUnit.HOURS.toMillis(2), "trnc0001");
        awaitNoQueuedEpochReclaim();
        plantAttemptArtifacts(lookup, deadAttempt);
        ageGenerations(dataDirectory, TimeUnit.HOURS.toMillis(2));
        final List<MemoryFileSummary> shortened = chain.subList(0, chain.size() - 1);
        writeGeneration(findGeneration(lookup, true), shortened, lookup);

        TimeSeriesEpochReclaim.reclaimObsoleteEpochs(lookup);

        Assertions.checkFalse(findAttemptDataFiles(lookup, deadAttempt).isEmpty(),
                "a newest generation that ends below the attested byte high-water [" + chain.get(chain.size() - 1)
                        .getCombinedMemoryFileSize() + " -> " + shortened.get(shortened.size() - 1)
                        .getCombinedMemoryFileSize() + "] must stop the sweep rather than delete what its tail named");
    }

    /**
     * Under a delta chain it is the base, not coverage, that decides what a generation costs: everything below the base
     * is a superseded snapshot, and the rewrite that moved the base re-emitted everything the live chain needs. A
     * generation below the crash fallback therefore goes even when it is the only record of some payload file - that
     * file is no longer part of any chain a reader can assemble, and keeping the generation would mean keeping a full
     * snapshot of a state the store has already left behind. Whether the file itself goes is the attempt rules' decision
     * (retention and liveness), pinned in the attempt tests; here it is fresh, so it stays.
     */
    @Test
    public void testAGenerationBelowTheFallbackIsPrunedEvenWhenItUniquelyNamesPayload() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("pruneUncovered");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        for (int i = 1; i <= 3; i++) {
            newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, 4 * i)), false).update().close();
        }
        final File dataDirectory = lookup.getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        final int chainBase = readChainBase(lookup);
        Assertions.checkTrue(chainBase >= 2,
                "the scenario needs a superseded range below the base, got base: " + chainBase);
        final FDate onlyValue = FDateBuilder.newDate(2020, 1, 2);
        final File onlyDataFile = new File(dataDirectory, "memory_plantedonly0001.data");
        Files.writeStringToFile(onlyDataFile, "planted", Charsets.defaultCharset());
        int lowestNumber = Integer.MAX_VALUE;
        for (final File generation : findGenerations(dataDirectory)) {
            final int generationNumber = AMemoryFileSummarySerializingCollection.indexNumberOf(generation.getName());
            if (generationNumber < lowestNumber) {
                lowestNumber = generationNumber;
            }
        }
        final File uncoveredGeneration = new File(dataDirectory,
                AMemoryFileSummarySerializingCollection.newIndexGenerationFileName(lowestNumber - 1));
        writeGeneration(uncoveredGeneration,
                List.of(new MemoryFileSummary(onlyValue, FDateSerde.GET, onlyValue, onlyValue, 0, 1,
                        onlyDataFile.getAbsolutePath(), 0, 0, 10L)),
                lookup);
        awaitNoQueuedEpochReclaim();
        ageGenerations(dataDirectory, TimeUnit.HOURS.toMillis(2));

        TimeSeriesEpochReclaim.reclaimObsoleteEpochs(lookup);

        Assertions.assertThat(uncoveredGeneration)
                .as("a snapshot below the crash fallback is superseded, whatever else it uniquely names").doesNotExist();
        Assertions.assertThat(onlyDataFile)
                .as("its payload is the attempt rules' decision, and this attempt is fresh").exists();
        Assertions.checkTrue(generationPresent(dataDirectory, chainBase), "the base stays");
        Assertions.checkTrue(generationPresent(dataDirectory, chainBase - 1), "and so does the crash fallback");
        Assertions.assertThat(readValues(table, KEY)).isEqualTo(newValues(1, 12));
    }

    /**
     * The lookup table's constructor used to delete an older generation as a side effect of reading the directory, on
     * the read path of every cache miss, while another live instance might still resolve that very file. Construction
     * now reads, and the reclaim owns deletion.
     */
    @Test
    public void testTableConstructionPrunesNothing() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("tableCtorPurity");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final File dataDirectory = lookup.getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        Files.forceMkdir(dataDirectory);
        //planted rather than published: a finished update submits this key's directory to the epoch sweep, and the
        //version-cleanup thread can then prune aged generations concurrently with the assertion below
        for (int i = 0; i < 3; i++) {
            final FDate value = FDateBuilder.newDate(2020, 1, i + 1);
            final File dataFile = new File(dataDirectory, "memory_170000000000" + i + "-ctpr0001.data");
            Files.writeStringToFile(dataFile, "planted", Charsets.defaultCharset());
            writeGeneration(new File(dataDirectory,
                    AMemoryFileSummarySerializingCollection.newIndexGenerationFileName(i)),
                    List.of(new MemoryFileSummary(value, FDateSerde.GET, value, value, 0, 1,
                            dataFile.getAbsolutePath(), 0, 0, 10L)),
                    lookup);
        }
        final List<File> before = findGenerations(dataDirectory);
        Assertions.assertThat(before).hasSize(3);
        //older than RETAIN_OBSOLETE_FILES_THRESHOLD (1 MONTH), which is what used to fire the constructor's delete
        awaitNoQueuedEpochReclaim();
        ageGenerations(dataDirectory, TimeUnit.DAYS.toMillis(61));

        try (ICloseableIterator<MemoryFileSummary> ignored = new VersionedTimeSeriesMemoryFileLookupTable<FDate>(
                lookup, dataDirectory, 0).range()) {
            Assertions.checkTrue(ignored.hasNext(), "a fresh table instance must see the published chain");
        }

        Assertions.assertThat(findGenerations(dataDirectory)).as("construction must not delete anything")
                .hasSize(before.size());
    }

    /**
     * A generation can be reclaimed while an instance that resolved it is still alive - the reclaim runs on the
     * version-cleanup thread, and on shared storage another process's reclaim can outrun this one. Re-resolving the
     * newest generation present is the recovery; the empty chain this used to answer with reads as an empty store, and
     * the trading derived-store guards take that as licence to delete and rebuild the key.
     */
    /**
     * A delta chain has exactly one full snapshot - its base - so the sweep must never prune it, and a store whose base
     * generation is gone must not be answered with the deltas that survive it. Those two are the same invariant seen
     * from either side: pruning the base is what would create the state in which a chain cannot be assembled, and a
     * chain that cannot be assembled is reported rather than served shorter, because a shorter answer is what the
     * trading derived-store guards take as licence to delete and rebuild the key. (The recoverable case - a lost newest
     * DELTA, which leaves the chain contiguous and one record shorter - is pinned at the table level in
     * VersionedTimeSeriesMemoryFileLookupTableTest.testATableWhoseNewestGenerationWasReclaimedMovesToTheOneBeforeIt.)
     */
    @Test
    public void testTheSweepKeepsTheChainBaseAndALostBaseIsReportedNotServedShorter() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("keepChainBase");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        //overlapping passes: each re-names the region the chain tail already names, so each rewrites and moves the base
        for (int i = 1; i <= 3; i++) {
            newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, 4 * i)), false).update().close();
        }
        final File dataDirectory = lookup.getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        final int chainBase = readChainBase(lookup);
        Assertions.checkTrue(chainBase >= 2,
                "the scenario needs a chain whose base is not its first generation, got base: " + chainBase);
        awaitNoQueuedEpochReclaim();
        ageGenerations(dataDirectory, TimeUnit.HOURS.toMillis(2));

        TimeSeriesEpochReclaim.reclaimObsoleteEpochs(lookup);

        Assertions.checkTrue(generationPresent(dataDirectory, chainBase),
                "the sweep prunes below the base, never the base itself");
        Assertions.checkTrue(generationPresent(dataDirectory, chainBase - 1),
                "and it keeps the generation below the base as the crash fallback");
        Assertions.checkFalse(readPublishedSummaries(lookup).isEmpty(),
                "the chain must still assemble after the sweep pruned the superseded snapshots");
        Assertions.assertThat(readValues(table, KEY)).as("the surviving chain must still serve every value")
                .isEqualTo(newValues(1, 12));

        final File baseGeneration = new File(dataDirectory,
                AMemoryFileSummarySerializingCollection.newIndexGenerationFileName(chainBase));
        Assertions.checkTrue(baseGeneration.delete(), "the damage case removes the base: " + baseGeneration);
        try (ICloseableIterator<MemoryFileSummary> ignored = lookup.getMemoryFileLookupTable().range()) {
            Assertions.fail("a chain whose only full snapshot is gone must not be served from its deltas");
        } catch (final IllegalStateException e) {
            Assertions.assertThat(e.getMessage()).as("the report must name the missing base")
                    .contains("Chain base [" + chainBase + "]");
        }
    }

    /**
     * What keeping the crash fallback is actually for: its chain may name payload the live chain no longer names - the
     * redo of a standalone '_incomplete_' shard re-serialises the region into a file of its own and drops the old one -
     * so its name set counts as referenced. Without that the sweep would reclaim the payload under the one generation
     * it just designated as the recovery path, and the fallback would be a file that no longer reads.
     */
    @Test
    public void testTheCrashFallbackKeepsThePayloadItsChainNames() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("keepFallbackPayload");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        for (int i = 1; i <= 4; i++) {
            newUpdater(table, KEY, WrapperCloseableIterable.maybeWrap(newValues(1, 4 * i)), false).update().close();
        }
        final File dataDirectory = lookup.getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        final int chainBase = readChainBase(lookup);
        Assertions.checkTrue(chainBase >= 3,
                "the scenario needs a superseded range below the crash fallback, got base: " + chainBase);

        final String deadAttempt = attemptIdOf(FDateMillis.nowMillis() - TimeUnit.HOURS.toMillis(2), "fbk0001");
        awaitNoQueuedEpochReclaim();
        plantAttemptArtifacts(lookup, deadAttempt);
        final File namedByFallback = new File(dataDirectory,
                MemoryFiles.EPOCH_LOCAL_DATA_FILE_PREFIX + deadAttempt + ".data");
        final File unnamedOfSameAttempt = new File(dataDirectory,
                MemoryFiles.EPOCH_LOCAL_DATA_FILE_PREFIX + deadAttempt + ".data_incomplete_0_planted.data");
        //the fallback generation's chain now names one of the dead attempt's files and nothing else does
        final FDate onlyValue = FDateBuilder.newDate(2020, 1, 2);
        writeGeneration(new File(dataDirectory,
                AMemoryFileSummarySerializingCollection.newIndexGenerationFileName(chainBase - 1)),
                List.of(new MemoryFileSummary(onlyValue, FDateSerde.GET, onlyValue, onlyValue, 0, 1,
                        namedByFallback.getAbsolutePath(), 0, 0, 7L)),
                lookup);
        ageGenerations(dataDirectory, TimeUnit.HOURS.toMillis(2));

        TimeSeriesEpochReclaim.reclaimObsoleteEpochs(lookup);

        Assertions.assertThat(namedByFallback)
                .as("payload the crash fallback names must survive, or the fallback cannot be read back").exists();
        Assertions.assertThat(unnamedOfSameAttempt)
                .as("the rest of that dead attempt's files are still garbage, and the sweep proves it read the names")
                .doesNotExist();
        Assertions.checkTrue(generationPresent(dataDirectory, chainBase), "the base stays");
        Assertions.checkTrue(generationPresent(dataDirectory, chainBase - 1), "the fallback stays");
        Assertions.assertThat(readValues(table, KEY)).isEqualTo(new ArrayList<>(newValues(1, 16)));
    }

    /**
     * The base of the live chain this key attests - the retention boundary the sweep prunes below.
     */
    private static int readChainBase(final TimeSeriesLookupStorageCache<String, FDate> lookup) {
        final MemoryFileMetadata metadata = lookup.getMemoryFileLookupTable().getMetadata();
        try (ICloseableProperties properties = metadata.getProperties()) {
            return metadata.getChainBase(properties);
        }
    }

    private static boolean generationPresent(final File dataDirectory, final int generationNumber) {
        final File generation = new File(dataDirectory,
                AMemoryFileSummarySerializingCollection.newIndexGenerationFileName(generationNumber));
        return generation.exists();
    }

    /**
     * The published index generations of a version data directory.
     */
    private static List<File> findGenerations(final File dataDirectory) {
        final List<File> found = new ArrayList<>();
        for (final File generation : VersionedTimeSeriesMemoryFileLookupTable.listIndexGenerations(dataDirectory)) {
            found.add(generation);
        }
        return found;
    }

    /**
     * Moves the mtimes of every generation of the directory back in time, which is what the in-flight bound and the
     * keep decision read.
     */
    /**
     * Settles the sweep that the test's own successful attempts submitted to the version-cleanup thread. Planting or
     * ageing state while one of those is still queued races it: the queued pass reads the marker the REAL attempt wrote
     * (its updateStart is 'now'), so an attempt id planted before that start is garbage to it and the planted files are
     * gone before this class' deterministic sweep call. Every scenario that plants therefore starts from an empty queue.
     */
    private static void awaitNoQueuedEpochReclaim() {
        TimeSeriesEpochReclaim.runPendingObsoleteEpochsReclaims();
        Assertions.assertThat(TimeSeriesEpochReclaim.getPendingObsoleteEpochsReclaimCount()).isZero();
    }

    private static void ageGenerations(final File dataDirectory, final long ageMillis) {
        final long agedMillis = FDateMillis.nowMillis() - ageMillis;
        for (final File generation : findGenerations(dataDirectory)) {
            Assertions.checkTrue(generation.setLastModified(agedMillis), "generation must be ageable: " + generation);
        }
    }

    /**
     * The data-file names every generation of the directory still names - the set a prune may never orphan.
     */
    private static List<String> namesOfAllGenerations(final TimeSeriesLookupStorageCache<String, FDate> lookup,
            final File dataDirectory) {
        final List<String> names = new ArrayList<>();
        for (final File generation : findGenerations(dataDirectory)) {
            try (AMemoryFileSummarySerializingCollection collection = new AMemoryFileSummarySerializingCollection(
                    new TextDescription("TimeSeriesEpochReclaimTest.namesOfAllGenerations: %s", generation),
                    AtomicNioFileChannel.newFile(generation.toURI()), true) {

                @Override
                protected MemoryFileSummarySerde newSerde() {
                    return new MemoryFileSummarySerde(lookup.getValueFixedLength());
                }

                @Override
                protected ICompressionFactory getCompressionFactory() {
                    return lookup.getCompressionFactory();
                }
            }; ICloseableIterator<MemoryFileSummary> it = collection.iterator()) {
                final List<MemoryFileSummary> summaries = new ArrayList<>();
                Lists.toListWithoutHasNext(it, summaries);
                for (final MemoryFileSummary summary : summaries) {
                    names.add(new File(summary.getMemoryResourceUri()).getName());
                }
            }
        }
        return names;
    }

    /**
     * Writes a generation file with the given chain, using the cache's own serde length and compression so the reclaim
     * reads it exactly as it reads a published one.
     */
    private static void writeGeneration(final File target, final List<MemoryFileSummary> summaries,
            final TimeSeriesLookupStorageCache<String, FDate> lookup) throws Exception {
        final Integer valueFixedLength = lookup.getValueFixedLength();
        final ICompressionFactory compressionFactory = lookup.getCompressionFactory();
        try (AMemoryFileSummarySerializingCollection collection = new AMemoryFileSummarySerializingCollection(
                new TextDescription("TimeSeriesEpochReclaimTest.writeGeneration: %s", target),
                AtomicNioFileChannel.newFile(target.toURI()), false) {

            @Override
            protected MemoryFileSummarySerde newSerde() {
                return new MemoryFileSummarySerde(valueFixedLength);
            }

            @Override
            protected ICompressionFactory getCompressionFactory() {
                return compressionFactory;
            }
        }) {
            for (final MemoryFileSummary summary : summaries) {
                collection.add(summary);
            }
            collection.closeWithEmptyWrite();
        }
    }

    /**
     * Writes garbage over one published index generation - the highest-numbered one when newest, otherwise the lowest,
     * so the two reliability tiers can be told apart.
     */
    private static File corruptGeneration(final TimeSeriesLookupStorageCache<String, FDate> lookup,
            final boolean newest) throws Exception {
        final File target = findGeneration(lookup, newest);
        Files.writeStringToFile(target, "not-a-collection-anymore", Charsets.defaultCharset());
        return target;
    }

    /**
     * The highest-numbered generation of the key's data directory when newest, otherwise the lowest - the two
     * reliability tiers are told apart by which one is damaged.
     */
    private static File findGeneration(final TimeSeriesLookupStorageCache<String, FDate> lookup,
            final boolean newest) {
        final File dataDirectory = lookup.getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        final List<File> generations = findGenerations(dataDirectory);
        Assertions.checkTrue(generations.size() >= 2,
                "the scenario needs at least two published generations, got: " + generations.size());
        File target = null;
        int best = newest ? Integer.MIN_VALUE : Integer.MAX_VALUE;
        for (final File generation : generations) {
            final int number = AMemoryFileSummarySerializingCollection.indexNumberOf(generation.getName());
            if (newest ? number > best : number < best) {
                best = number;
                target = generation;
            }
        }
        return target;
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
