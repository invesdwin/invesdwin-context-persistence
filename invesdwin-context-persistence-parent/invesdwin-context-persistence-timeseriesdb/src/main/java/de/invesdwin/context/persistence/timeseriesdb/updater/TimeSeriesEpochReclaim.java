package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannel;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.lease.TimeSeriesDirectoryHashKeyVersionLeaseRegistry;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummarySerde;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFiles;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.AMemoryFileSummarySerializingCollection;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.MemoryFileMetadata;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.VersionedTimeSeriesMemoryFileLookupTable;
import de.invesdwin.context.system.properties.ICloseableProperties;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLockRegistry;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.string.Charsets;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.math.Longs;
import de.invesdwin.util.time.date.millis.FDateMillis;

/**
 * Reclaim of the data files an update attempt leaves behind (the M1 attempt-local layout is what makes this a hard
 * dependency): every attempt writes data files named after itself
 * ('memory_&lt;attemptId&gt;.data[.&lt;offset&gt;]' including its '_incomplete_' variants) in the version's data
 * directory - the cross-attempt '_incomplete_' sweep had been removed because it deleted files the published generation
 * references, so without this reclaim every attempt accumulates garbage. Reclaim runs without a new global timer:
 * <ul>
 * <li>{@link #reclaimAbortedAttemptEpoch(TimeSeriesLookupStorageCache, String)} runs in the failing writer's abort
 * path: what the attempt staged after its last published boundary is provably unpinned garbage (the published
 * generation reference check still guards the bytes a boundary commit published before the attempt died, and the rare
 * case of a commit landing inside a throwing close);</li>
 * <li>{@link #scheduleObsoleteEpochsReclaim(TimeSeriesLookupStorageCache)} is what the completion of an update attempt
 * calls: it records the key's data directory and hands the sweep to the version-cleanup thread of
 * {@link TimeSeriesDirectoryHashKeyVersionLeaseRegistry}, which already has its own single thread and lifecycle. That
 * is not a cosmetic move: a sweep reads every '&lt;N&gt;_memory.index' generation of the key to learn which data files
 * are still referenced, and a live key's directory holds generations back to
 * {@link TimeSeriesProperties#RETAIN_OBSOLETE_FILES_THRESHOLD} - measured on ptstaging with 1,496 generations of
 * ~578 KB in one live key's directory, so one sweep decompresses ~865 MB. Doing that inline held the live cache update
 * lock for minutes while every demand of that key answered retry-later.</li>
 * </ul>
 * The reference set is collected from ALL '&lt;N&gt;_memory.index' generations still present on disk (not just the
 * newest): older generations are retained for the length of the longest backtest (RETAIN_OBSOLETE_FILES_THRESHOLD)
 * and open iterations resolve one of them, so a file named by any existing generation must stay readable. The sweep
 * builds that set and the directory listing ONCE for all candidate attempts of the directory, because both depend on
 * the directory and not on the attempt.
 * <p>
 * The same walk also prunes index generations (this is the class that owns generation deletion;
 * {@link VersionedTimeSeriesMemoryFileLookupTable}'s constructor no longer deletes anything, because it sits on the
 * read path of every cache miss and a generation it removed from under a live instance made that instance read an
 * empty chain). The rule is coverage, not age: generations are walked newest-first, the newest
 * {@link #INDEX_GENERATIONS_KEEP_FLOOR} are always kept as the recovery margin for a publish that died mid-write, and
 * an older one is deleted only when every data file it names is already named by a generation that stays - where
 * "names" is filtered to the files that actually exist, because every pass also names the '_incomplete_' marker of the
 * file it is writing and a completed pass renames that marker away, which would otherwise leave every generation
 * holding one name nothing else has and make nothing prunable (measured on a five-generation test store: prunable=0,
 * covered=false at every step, before the filter). That makes
 * the deleted set provably name-redundant under the cumulative layout (each publish streams the previous chain into
 * the new file), while the generations that still carry a name nothing else carries - the redo of a standalone
 * '_incomplete_' shard replaces one summary, and the replaced file leaves the chain with it - stay. A generation
 * younger than {@link TimeSeriesProperties#RETAIN_OBSOLETE_EPOCHS_THRESHOLD} is never deleted here: a publisher may
 * still be writing it, and a generation whose write is interrupted by an unlink does not come back (the writer keeps
 * the unlinked inode, so its publish vanishes). As with the attempt sweep, an unreliable walk deletes nothing.
 * </p>
 */
@ThreadSafe
public final class TimeSeriesEpochReclaim {

    /**
     * The number of newest index generations that are never pruned, whatever they name. A publish writes the new
     * generation and only then becomes the current view, so the generation before it is the last intact chain a store
     * can fall back to when a publish dies mid-write; below this floor there is nothing left to fall back to.
     */
    static final int INDEX_GENERATIONS_KEEP_FLOOR = 2;

    private static final Log LOG = new Log(TimeSeriesEpochReclaim.class);

    /**
     * The data directories a finished update attempt asked to have swept, keyed by the directory itself (a File
     * compares by path, so repeated attempts on one key coalesce into one sweep). The values carry everything the sweep
     * needs, taken from the cache while the calling thread still owns it: {@link TimeSeriesLookupStorageCache} is
     * &#64;NotThreadSafe and the sweep runs on another thread.
     */
    private static final Map<File, PendingReclaim> PENDING_RECLAIMS = ILockCollectionFactory.getInstance(true)
            .newConcurrentMap();

    private TimeSeriesEpochReclaim() {}

    /**
     * Best-effort, never throws: delete the given attempt's own artifacts (this attempt just aborted). Kept safe by the
     * published-generation reference check, so a commit that landed before a throwing transaction close is not undone.
     */
    public static void reclaimAbortedAttemptEpoch(final TimeSeriesLookupStorageCache<?, ?> cache,
            final String attemptId) {
        if (attemptId == null) {
            //the attempt failed before its update transaction was created: nothing attempt local to reclaim
            return;
        }
        try {
            final File dataDirectory = cache.getDirectoryHashKeyVersionMemory()
                    .getDirectoryHashKeyVersionDataShared();
            final File[] attemptDataFiles = listAttemptDataFiles(dataDirectory, attemptId);
            if (attemptDataFiles.length == 0) {
                return;
            }
            final File[] directoryFiles = dataDirectory.listFiles();
            if (directoryFiles == null) {
                return;
            }
            final List<File> generationFiles = new ArrayList<>();
            final Set<String> existingNames = ILockCollectionFactory.getInstance(false).newLinkedSet();
            for (final File directoryFile : directoryFiles) {
                existingNames.add(directoryFile.getName());
                if (AMemoryFileSummarySerializingCollection.isIndexGenerationFileName(directoryFile.getName())) {
                    generationFiles.add(directoryFile);
                }
            }
            final ReferenceSet referencedNames = collectPublishedDataFileNames(dataDirectory, generationFiles,
                    existingNames, cache.getValueFixedLength(), cache.getCompressionFactory());
            if (!referencedNames.isReliable() && referencedNames.isGenerationsPresent()) {
                //The protection this method claims ("a commit that landed before a throwing transaction close is not
                //undone") IS the reference set, so an incomplete one must stop the delete rather than permit it.
                LOG.warn("Not reclaiming the aborted attempt [%s] of key [%s]: %s", attemptId, cache.getHashKey(),
                        referencedNames.getUnreliableReason());
                return;
            }
            deleteAttemptArtifacts(attemptDataFiles, referencedNames.getNames());
        } catch (final Throwable t) {
            //reclaim is an optimization, never a cause of failure
            LOG.warn("Best-effort reclaim of the aborted attempt [%s] of key [%s] failed: %s", attemptId,
                    cache.getHashKey(), Throwables.concatMessages(t));
        }
    }

    /**
     * The synchronous form of the sweep - one data directory, all dead attempts of it. The update path calls
     * {@link #scheduleObsoleteEpochsReclaim(TimeSeriesLookupStorageCache)} instead; this stays the entry point for a
     * caller that must have the garbage gone before it returns (and for the tests).
     */
    public static void reclaimObsoleteEpochs(final TimeSeriesLookupStorageCache<?, ?> cache) {
        reclaimObsoleteEpochs(newPendingReclaim(cache));
    }

    /**
     * Best-effort, never throws: record this key's data directory for the next sweep on the version-cleanup thread and
     * ask that thread to run. Cheap on the calling thread by design: one directory listing is left to the sweeper,
     * which is the point (see the class javadoc for what a sweep costs).
     */
    public static void scheduleObsoleteEpochsReclaim(final TimeSeriesLookupStorageCache<?, ?> cache) {
        if (enqueueObsoleteEpochsReclaim(cache)) {
            TimeSeriesDirectoryHashKeyVersionLeaseRegistry.requestEpochReclaim();
        }
    }

    /**
     * Records the sweep without asking for it: the queue is keyed by data directory, so any number of finished attempts
     * of one key coalesce into one sweep.
     *
     * @return true when this submission is what made the directory pending
     */
    static boolean enqueueObsoleteEpochsReclaim(final TimeSeriesLookupStorageCache<?, ?> cache) {
        try {
            final PendingReclaim pending = newPendingReclaim(cache);
            return pending != null && PENDING_RECLAIMS.putIfAbsent(pending.getDataDirectory(), pending) == null;
        } catch (final Throwable t) {
            //reclaim is an optimization, never a cause of failure
            LOG.warn("Best-effort reclaim scheduling of key [%s] failed: %s", cache.getHashKey(),
                    Throwables.concatMessages(t));
            return false;
        }
    }

    static int getPendingObsoleteEpochsReclaimCount() {
        return PENDING_RECLAIMS.size();
    }

    /**
     * Sweeps every data directory submitted since the last run: the entry is removed BEFORE its sweep, so a submission
     * made while this runs is not lost (it simply queues for the next run). Called from the version-cleanup thread.
     */
    public static void runPendingObsoleteEpochsReclaims() {
        if (PENDING_RECLAIMS.isEmpty()) {
            return;
        }
        for (final File dataDirectory : new ArrayList<>(PENDING_RECLAIMS.keySet())) {
            final PendingReclaim pending = PENDING_RECLAIMS.remove(dataDirectory);
            if (pending == null) {
                //another drain took this directory already
                continue;
            }
            reclaimObsoleteEpochs(pending);
        }
    }

    public static boolean hasPendingObsoleteEpochsReclaims() {
        return !PENDING_RECLAIMS.isEmpty();
    }

    /**
     * Everything a sweep needs of one key's data directory, read off the cache on the submitting thread.
     */
    private static PendingReclaim newPendingReclaim(final TimeSeriesLookupStorageCache<?, ?> cache) {
        final File dataDirectory = cache.getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        if (dataDirectory == null) {
            return null;
        }
        return new PendingReclaim(cache.getHashKey(), dataDirectory, cache.getUpdateProgressFile(),
                cache.getValueFixedLength(), cache.getCompressionFactory());
    }

    private static void reclaimObsoleteEpochs(final PendingReclaim pending) {
        try {
            final File dataDirectory = pending.getDataDirectory();
            //ONE listing for the whole directory, bucketed in memory: the attempt garbage grouped by the attempt id it
            //carries, the index generations, and the set of names that actually exist. Listing per attempt made the
            //sweep quadratic in the directory's file count (measured: 2,992 entries in one live key's directory). A
            //null listing means the directory is gone - the version janitor rm -rf's whole version directories on this
            //very thread (TimeSeriesDirectoryHashKeyVersionLease:184,:190) - and that reads as "keep everything",
            //never as "names nothing".
            final File[] directoryFiles = dataDirectory.listFiles();
            if (directoryFiles == null) {
                LOG.debug("Data directory [%s] of key [%s] is gone, nothing to sweep", dataDirectory,
                        pending.getHashKey());
                return;
            }
            final Set<String> existingNames = ILockCollectionFactory.getInstance(true).newLinkedSet();
            final List<File> generationFiles = new ArrayList<>();
            final Map<String, List<File>> filesByAttemptId = ILockCollectionFactory.getInstance(false).newLinkedMap();
            for (final File directoryFile : directoryFiles) {
                final String name = directoryFile.getName();
                existingNames.add(name);
                if (AMemoryFileSummarySerializingCollection.isIndexGenerationFileName(name)) {
                    generationFiles.add(directoryFile);
                    continue;
                }
                if (MemoryFiles.isEpochLocalDataFile(name)) {
                    final String attemptId = attemptIdOfDataFileName(name);
                    if (attemptId != null) {
                        filesByAttemptId.computeIfAbsent(attemptId, k -> new ArrayList<>()).add(directoryFile);
                    }
                }
            }
            final boolean generationsToPrune = generationFiles.size() > INDEX_GENERATIONS_KEEP_FLOOR;
            if (filesByAttemptId.isEmpty() && !generationsToPrune) {
                return;
            }
            //ONE reference set for the whole sweep, built lazily: it depends on the directory, not on the attempt, and
            //reading it decompresses every retained index generation of the key.
            ReferenceSet referencedNames = null;
            //ONE read of this key's progress marker and heartbeat claim for the same reason.
            final TimeSeriesUpdaterProgress progress = TimeSeriesUpdaterProgress.readUpdateProgress(
                    pending.getUpdateProgressFile());
            if (generationsToPrune) {
                referencedNames = collectPublishedDataFileNames(dataDirectory, generationFiles, existingNames,
                        pending.getValueFixedLength(), pending.getCompressionFactory());
                //First the generations, so that the payload names a pruned generation was the last record of are
                //visible to the garbage sweep in this same pass instead of surviving until the next one.
                pruneRedundantIndexGenerations(pending, progress, referencedNames, generationFiles.size());
            }
            for (final Map.Entry<String, List<File>> entry : filesByAttemptId.entrySet()) {
                final String attemptId = entry.getKey();
                if (isAttemptLive(pending, progress, attemptId)) {
                    //owned by the heartbeat-live attempt this key's marker describes: never touch
                    continue;
                }
                final File[] dataFiles = entry.getValue().toArray(new File[0]);
                if (!TimeSeriesProperties.RETAIN_OBSOLETE_EPOCHS_THRESHOLD.isLessThanMillis(
                        attemptAgeMillis(attemptId, dataFiles))) {
                    //younger than the retention threshold: a reader may still be resolving it
                    continue;
                }
                if (referencedNames == null) {
                    referencedNames = collectPublishedDataFileNames(dataDirectory, generationFiles, existingNames,
                            pending.getValueFixedLength(), pending.getCompressionFactory());
                }
                if (!referencedNames.isReliable()) {
                    //Fail closed here rather than best-effort: the reference set is the ONLY evidence of what the
                    //published index still names, and every published payload file carries an attempt id, so an
                    //incomplete set does not merely skip protection - it deletes payload the store believes it
                    //published. The next completed attempt re-submits this key, so skipping costs a delay only.
                    LOG.warn("Skipping the attempt reclaim of key [%s]: %s - %s attempt group(s) left in place",
                            pending.getHashKey(), referencedNames.getUnreliableReason(), filesByAttemptId.size());
                    return;
                }
                deleteAttemptArtifacts(dataFiles, referencedNames.getNames());
            }
        } catch (final Throwable t) {
            //reclaim is an optimization, never a cause of failure
            LOG.warn("Best-effort attempt reclaim sweep of key [%s] failed: %s", pending.getHashKey(),
                    Throwables.concatMessages(t));
        }
    }

    /**
     * Deletes the index generations the walk proved redundant. Never on an unreliable walk, and never while an attempt
     * of this key is live: a generation that is unlinked while its writer holds it is not re-created, so the publish
     * that was being written into it vanishes (the writer keeps the unlinked inode). A refused unlink is treated as a
     * retention miss and retried by the next sweep, not as damage. NOTE for the Windows side of this store: java.nio
     * opens with FILE_SHARE_DELETE by default (JDK 17 WindowsChannelFactory.java:80-82 set shareRead/shareWrite/
     * shareDelete true, :114-122 only turn them off for explicit sharing options, :245-250 map them to the Win32
     * flags), so a delete of a generation some process still has open does NOT fail there - the name goes away and
     * the open handle keeps working. The protection is therefore the coverage rule and the in-flight guard above, not
     * any expectation of a sharing violation.
     */
    private static void pruneRedundantIndexGenerations(final PendingReclaim pending,
            final TimeSeriesUpdaterProgress progress, final ReferenceSet referenceSet, final int generationCount) {
        final List<File> prunable = referenceSet.getPrunableGenerations();
        if (prunable.isEmpty()) {
            return;
        }
        if (!referenceSet.isReliable()) {
            LOG.warn("Not pruning index generations of key [%s]: %s", pending.getHashKey(),
                    referenceSet.getUnreliableReason());
            return;
        }
        if (isKeyAttemptLive(pending, progress)) {
            LOG.debug("Not pruning index generations of key [%s]: an update attempt of this key is live",
                    pending.getHashKey());
            return;
        }
        int deleted = 0;
        for (final File prunedGeneration : prunable) {
            if (Files.deleteQuietly(prunedGeneration)) {
                deleted++;
            }
        }
        LOG.info("Pruned %s redundant index generation(s) of key [%s], %s of %s kept", deleted, pending.getHashKey(),
                generationCount - deleted, generationCount);
    }

    /**
     * Whether this key's progress marker describes an update attempt whose heartbeat is still fresh, regardless of
     * which attempt produced the candidate file - the generation prune must not overlap any live publisher of the key,
     * not merely one that postdates a given attempt id.
     */
    private static boolean isKeyAttemptLive(final PendingReclaim pending, final TimeSeriesUpdaterProgress progress) {
        if (progress == null || progress.getUpdateStart() == null) {
            return false;
        }
        return isHeartbeatLive(progress.getOwner(),
                new File(pending.getUpdateProgressFile().getAbsolutePath() + ".lock"));
    }

    /**
     * The attempt id embedded in an attempt local data file name: 'memory_&lt;attemptId&gt;' up to the first dot (the
     * extensions are '.data', '.data.&lt;preceding offset&gt;') or up to the '_incomplete_' marker (the standalone
     * file of an attempt has the marker inside the base name, so the id ends there), null for names that do not carry
     * one (a data file without an attempt id never passes {@link MemoryFiles#isEpochLocalDataFile(String)} anyway).
     */
    private static String attemptIdOfDataFileName(final String fileName) {
        final int start = MemoryFiles.EPOCH_LOCAL_DATA_FILE_PREFIX.length();
        int end = fileName.indexOf('.', start);
        final int marker = fileName.indexOf(MemoryFiles.INCOMPLETE_MEMORY_FILE_MARKER, start);
        if (marker > start && (end < 0 || marker < end)) {
            end = marker;
        }
        return end > start ? fileName.substring(start, end) : null;
    }

    /**
     * The attempt local data files of the version's data directory, all of them when attemptId is null and only the
     * ones of that attempt otherwise (exact 'memory_&lt;attemptId&gt;' prefix match, which covers '.data',
     * '.data.&lt;offset&gt;' and the '_incomplete_' variants of that attempt and can never hit another attempt's
     * files).
     */
    private static File[] listAttemptDataFiles(final File dataDirectory, final String attemptId) {
        final File[] files = dataDirectory.listFiles((dir, name) -> {
            if (attemptId != null) {
                return name.startsWith(MemoryFiles.EPOCH_LOCAL_DATA_FILE_PREFIX + attemptId);
            } else {
                return MemoryFiles.isEpochLocalDataFile(name);
            }
        });
        return files != null ? files : new File[0];
    }

    /**
     * The data file names that every '&lt;N&gt;_memory.index' generation still present in the version's data
     * directory names (the newest published view plus the older generations retained for open iterations). This is the
     * trap M1 flagged: a file any existing generation names must stay readable, a file no generation names is
     * garbage. The result also carries whether that picture came out COMPLETE, because the caller deletes on it: the
     * name set is the only record of what the published index refers to, and every payload file of the store carries
     * an attempt id, so a name that is missing from the set does not read as "unknown" but as garbage.
     */
    private static ReferenceSet collectPublishedDataFileNames(final File dataDirectory,
            final List<File> generationFiles, final Set<String> existingNames, final Integer valueFixedLength,
            final ICompressionFactory compressionFactory) {
        final Set<String> referencedNames = ILockCollectionFactory.getInstance(true).newLinkedSet();
        if (generationFiles.isEmpty()) {
            //An index-less directory whose payload still carries attempt ids is damage or a build that writes another
            //layout; either way "names nothing" must not become "delete everything".
            return ReferenceSet.unreliable(referencedNames, "no '"
                    + AMemoryFileSummarySerializingCollection.MEMORY_INDEX_FILE_NAME + "' generation is present",
                    false, List.of());
        }
        final int chainBase = readAttestedChainBase(dataDirectory);
        if (chainBase >= 0) {
            //The delta layout: the live chain is a contiguous generation range, so reading that range once IS a
            //complete name set and everything below the base is a superseded snapshot.
            return collectChainReferenceSet(dataDirectory, generationFiles, valueFixedLength, compressionFactory,
                    chainBase);
        }
        //Walked newest generation first, so that 'referencedNames' is always the name set of what the sweep KEEPS: an
        //older generation whose names are already in it adds nothing the store needs, and it is the one entry whose
        //removal the walk can therefore justify. An older generation that still names a file nothing kept names
        //stays, which is what protects the redo case (a publish that replaces the last summary of its predecessor
        //drops that file from its own chain, so the older chain is the only remaining record of it).
        final List<File> orderedGenerations = new ArrayList<>(generationFiles);
        orderedGenerations.sort(Comparator.comparingInt(
                (File f) -> AMemoryFileSummarySerializingCollection.indexNumberOf(f.getName())).reversed());
        final long nowMillis = FDateMillis.nowMillis();
        final List<File> prunable = new ArrayList<>();
        final Set<String> newestGenerationNames = ILockCollectionFactory.getInstance(false).newLinkedSet();
        int kept = 0;
        for (int i = 0; i < orderedGenerations.size(); i++) {
            final File generationFile = orderedGenerations.get(i);
            final List<MemoryFileSummary> summaries = readIndexGenerationSummaries(valueFixedLength, compressionFactory,
                    generationFile);
            if (summaries == null) {
                if (i == 0) {
                    //The newest generation is the only record of the most recent publishes - older ones cannot name
                    //what the newest has not written yet - so losing it makes the set incomplete rather than smaller.
                    return ReferenceSet.unreliable(referencedNames, "the newest generation ["
                            + generationFile.getName() + "] does not parse", true, prunable);
                }
                //An unreadable OLDER generation is tolerable for the name set because every publish streams the
                //previous chain into the new file, so the newest view carries the older generations' file names; only
                //their offsets and sizes get re-written. Kept visible because several such files in one directory is
                //store damage. NOTE this tolerance is a property of the CUMULATIVE layout, not of this method: if the
                //index ever becomes a set of delta segments, an unreadable member names data that nothing else names,
                //and this branch has to become the fatal one (raised by the design panel's verdict on the segment
                //proposals). It is never prunable either: an unreadable chain is not a proven-redundant one.
                LOG.warn("Reclaim of data directory [%s] proceeds despite an unreadable older generation [%s]",
                        dataDirectory, generationFile.getName());
                kept++;
                continue;
            }
            if (i == 0 && !newestGenerationReachesTheAttestedHighWater(dataDirectory, summaries)) {
                //A newest generation truncated at a record boundary still parses as a valid, shorter collection, so
                //neither of the two tiers above fires on it and the walk would name less than the store published.
                //The store already persists the byte high-water of the chain it published on every put
                //(EXPECTED_MEMORY_FILE_SIZE, read back by isEmptyOrInconsistent), so a chain that does not reach it is
                //not the published chain, and pruning or reclaiming on it is the fail-open case this class exists to
                //close.
                return ReferenceSet.unreliable(referencedNames, "the newest generation ["
                        + generationFile.getName() + "] does not reach the attested byte high-water", true, prunable);
            }
            if (kept < INDEX_GENERATIONS_KEEP_FLOOR) {
                addSummaryNames(referencedNames, summaries);
                if (i == 0) {
                    //the current view: an '_incomplete_' name it carries belongs to a chunk the store still points at,
                    //so it keeps the generation that names it irreplaceable
                    addSummaryNames(newestGenerationNames, summaries);
                }
                kept++;
                continue;
            }
            final Set<String> ownNames = ILockCollectionFactory.getInstance(false).newLinkedSet();
            addSummaryNames(ownNames, summaries);
            //Only a name that still exists on disk can make a generation irreplaceable, and an '_incomplete_' name
            //counts only while the current view still carries it. An in-flight chunk is written under its
            //'_incomplete_' name (MemoryFiles.newIncompleteMemoryFile) and moves to its final name when the chunk
            //closes, so an '_incomplete_' name held only by OLDER chains is the residue of a dead pass. Measured on a
            //five-generation append store: every generation's unique name is exactly its own pass's '_incomplete_'
            //file, and all of those files are still on disk - without this filter nothing is ever prunable.
            ownNames.retainAll(existingNames);
            ownNames.removeIf(name -> MemoryFiles.isIncompleteMemoryFile(name)
                    && !newestGenerationNames.contains(name));
            final boolean coveredByKept = referencedNames.containsAll(ownNames);
            final boolean tooYoungToTouch = !TimeSeriesProperties.RETAIN_OBSOLETE_EPOCHS_THRESHOLD
                    .isLessThanMillis(nowMillis - generationFile.lastModified());
            if (coveredByKept && !tooYoungToTouch) {
                prunable.add(generationFile);
            } else {
                //either it still names something the kept set does not, or a publisher may be in it: keep it, and keep
                //naming what it names so a later sweep's coverage test sees it
                referencedNames.addAll(ownNames);
                kept++;
            }
        }
        if (referencedNames.isEmpty() && hasPublishedBytesWithoutIndex(dataDirectory)) {
            //Published payload that no index names is damage (a truncated newest generation that still parses as an
            //empty collection, or a build writing another layout), and deleting on that reading is the fail-open case.
            return ReferenceSet.unreliable(referencedNames,
                    "no generation names any of the published data files present", true, List.of());
        }
        return ReferenceSet.reliable(referencedNames, prunable);
    }

    /**
     * The base of the live chain this key attests, or {@link MemoryFileMetadata#MISSING_CHAIN_BASE} when it attests
     * none - which is a store still on the cumulative layout, and also the answer when the attestation cannot be read,
     * because the cumulative walk is the conservative one (it keeps anything it cannot prove redundant).
     */
    private static int readAttestedChainBase(final File dataDirectory) {
        final MemoryFileMetadata metadata = new MemoryFileMetadata(dataDirectory);
        try (ICloseableProperties properties = metadata.getProperties()) {
            return metadata.getChainBase(properties);
        } catch (final Throwable t) {
            LOG.warn("The chain attestation of data directory [%s] cannot be read: %s", dataDirectory,
                    Throwables.concatMessages(t));
            return MemoryFileMetadata.MISSING_CHAIN_BASE;
        }
    }

    /**
     * The reference set of a delta chain: the attested base generation and every generation above it, read once. That
     * range is the live chain, so its name set is complete by construction and everything below the base is a
     * superseded snapshot - prunable by number, without the coverage proof the cumulative layout needs, because a
     * rewrite re-emits the whole chain it replaces. The highest generation below the base is kept as the crash
     * fallback: it is the last intact chain should the new base ever prove unusable, and its own name set is added to
     * the reference set, because a fallback whose payload has been reclaimed is not a fallback.
     *
     * <p>
     * Two differences from the cumulative walk, both demanded by the layout. An unreadable member is FATAL rather than
     * tolerated, because a delta names payload that no other generation names - a reference set that missed one would
     * delete live data, and the cumulative walk's own note asks for exactly this inversion once the index becomes delta
     * segments. And a hole inside the attested range is damage rather than a shorter chain: nothing legitimately
     * removes a generation between the base and the newest.
     */
    private static ReferenceSet collectChainReferenceSet(final File dataDirectory, final List<File> generationFiles,
            final Integer valueFixedLength, final ICompressionFactory compressionFactory, final int chainBase) {
        final Set<String> referencedNames = ILockCollectionFactory.getInstance(true).newLinkedSet();
        final List<File> ordered = orderedGenerationsByNumber(generationFiles);
        if (ordered.isEmpty()) {
            return ReferenceSet.unreliable(referencedNames, "no generation is named by the attested chain base", false,
                    List.of());
        }
        final int newestNumber = AMemoryFileSummarySerializingCollection
                .indexNumberOf(ordered.get(ordered.size() - 1).getName());
        if (chainBase > newestNumber) {
            return ReferenceSet.unreliable(referencedNames, "the attested chain base [" + chainBase
                    + "] is above the newest generation present [" + newestNumber + "]", true, List.of());
        }
        final int baseIndex = indexOfGenerationNumber(ordered, chainBase);
        if (baseIndex < 0) {
            return ReferenceSet.unreliable(referencedNames, "the attested chain range [" + chainBase + ".."
                    + newestNumber + "] is missing generation [" + chainBase + "]", true, List.of());
        }
        final String liveChainFailure = addLiveChainNames(referencedNames, ordered, baseIndex, chainBase, newestNumber,
                dataDirectory, valueFixedLength, compressionFactory);
        if (liveChainFailure != null) {
            return ReferenceSet.unreliable(referencedNames, liveChainFailure, true, List.of());
        }
        final String fallbackFailure = addFallbackChainNames(referencedNames, ordered, baseIndex, valueFixedLength,
                compressionFactory);
        if (fallbackFailure != null) {
            return ReferenceSet.unreliable(referencedNames, fallbackFailure, true, List.of());
        }
        if (referencedNames.isEmpty() && hasPublishedBytesWithoutIndex(dataDirectory)) {
            return ReferenceSet.unreliable(referencedNames,
                    "no generation names any of the published data files present", true, List.of());
        }
        return ReferenceSet.reliable(referencedNames, getSupersededGenerations(ordered, baseIndex));
    }

    /**
     * The generation files of this directory in generation-number order; names that carry no generation number are not
     * generations and are dropped.
     */
    private static List<File> orderedGenerationsByNumber(final List<File> generationFiles) {
        final List<File> ordered = new ArrayList<>(generationFiles);
        ordered.removeIf(f -> AMemoryFileSummarySerializingCollection.indexNumberOf(f.getName()) == Integer.MIN_VALUE);
        ordered.sort(Comparator.comparingInt(
                (File f) -> AMemoryFileSummarySerializingCollection.indexNumberOf(f.getName())));
        return ordered;
    }

    private static int indexOfGenerationNumber(final List<File> orderedGenerations, final int generationNumber) {
        for (int i = 0; i < orderedGenerations.size(); i++) {
            if (AMemoryFileSummarySerializingCollection.indexNumberOf(orderedGenerations.get(i).getName()) == generationNumber) {
                return i;
            }
        }
        return -1;
    }

    /**
     * The name set of the live chain: the attested base and every generation above it. A hole inside the range or an
     * unreadable member answers with the reason to refuse the sweep, and so does a chain that does not reach the byte
     * high-water this key attests - the same gate the cumulative walk applies to its newest view, because a chain below
     * it is not the chain the store published and pruning or reclaiming on it is the fail-open case.
     */
    private static String addLiveChainNames(final Set<String> referencedNames, final List<File> orderedGenerations,
            final int baseIndex, final int chainBase, final int newestNumber, final File dataDirectory,
            final Integer valueFixedLength, final ICompressionFactory compressionFactory) {
        int expectedNumber = chainBase;
        List<MemoryFileSummary> newestSummaries = null;
        for (int i = baseIndex; i < orderedGenerations.size(); i++) {
            final File generationFile = orderedGenerations.get(i);
            final int number = AMemoryFileSummarySerializingCollection.indexNumberOf(generationFile.getName());
            if (number != expectedNumber) {
                return "the attested chain range [" + chainBase + ".." + newestNumber + "] is missing generation ["
                        + expectedNumber + "]";
            }
            expectedNumber = number + 1;
            final List<MemoryFileSummary> summaries = readIndexGenerationSummaries(valueFixedLength,
                    compressionFactory, generationFile);
            if (summaries == null) {
                return "the chain member [" + generationFile.getName() + "] does not parse";
            }
            addSummaryNames(referencedNames, summaries);
            newestSummaries = summaries;
        }
        if (newestSummaries != null && !newestGenerationReachesTheAttestedHighWater(dataDirectory, newestSummaries)) {
            return "the chain [" + chainBase + ".." + newestNumber + "] does not reach the attested byte high-water";
        }
        return null;
    }

    /**
     * The name set of the crash fallback - the highest generation below the base. It is only a fallback while the
     * payload it names is still there: its chain may name a file the rewrite dropped from the live chain (the redo of
     * a standalone '_incomplete_' shard re-serialises the region into a file of its own), so its names count as
     * referenced even though no live reader asks for them. Reading it is one extra generation per sweep.
     */
    private static String addFallbackChainNames(final Set<String> referencedNames,
            final List<File> orderedGenerations, final int baseIndex, final Integer valueFixedLength,
            final ICompressionFactory compressionFactory) {
        if (baseIndex == 0) {
            return null;
        }
        final File fallbackGeneration = orderedGenerations.get(baseIndex - 1);
        final List<MemoryFileSummary> fallbackSummaries = readIndexGenerationSummaries(valueFixedLength,
                compressionFactory, fallbackGeneration);
        if (fallbackSummaries == null) {
            return "the fallback generation [" + fallbackGeneration.getName() + "] does not parse";
        }
        addSummaryNames(referencedNames, fallbackSummaries);
        return null;
    }

    /**
     * The generations below the crash fallback: superseded snapshots, old enough that no reader is still resolving
     * them. The fallback itself is never in this list, and neither is anything at or above the base.
     */
    private static List<File> getSupersededGenerations(final List<File> orderedGenerations, final int baseIndex) {
        final long nowMillis = FDateMillis.nowMillis();
        final List<File> prunable = new ArrayList<>();
        for (int i = 0; i < baseIndex - 1; i++) {
            final File generation = orderedGenerations.get(i);
            if (!TimeSeriesProperties.RETAIN_OBSOLETE_EPOCHS_THRESHOLD.isLessThanMillis(
                    nowMillis - generation.lastModified())) {
                //younger than the retention threshold: a reader may still be resolving it
                continue;
            }
            prunable.add(generation);
        }
        return prunable;
    }

    /**
     * Whether the chain of the newest generation reaches the byte high-water this key's metadata attests - the same
     * equality {@link TimeSeriesLookupStorageCache#isEmptyOrInconsistent()} uses as its own integrity test, applied
     * here to the file the walk is about to trust. A store that never published has no attestation, which is not a
     * mismatch; a read failure answers false, because the caller deletes nothing unless the walk is reliable.
     */
    private static boolean newestGenerationReachesTheAttestedHighWater(final File dataDirectory,
            final List<MemoryFileSummary> summaries) {
        final MemoryFileMetadata metadata = new MemoryFileMetadata(dataDirectory);
        try (ICloseableProperties properties = metadata.getProperties()) {
            final long attestedHighWater = metadata.getExpectedMemoryFileSize(properties);
            if (attestedHighWater == MemoryFileMetadata.MISSING_EXPECTED_MEMORY_FILE_SIZE) {
                return true;
            }
            final long chainHighWater = summaries.isEmpty() ? 0L
                    : summaries.get(summaries.size() - 1).getCombinedMemoryFileSize();
            if (chainHighWater != attestedHighWater) {
                LOG.warn("The newest index generation of data directory [%s] ends at [%s] bytes, the key attests [%s]",
                        dataDirectory, chainHighWater, attestedHighWater);
            }
            return chainHighWater == attestedHighWater;
        } catch (final Throwable t) {
            LOG.warn("The byte attestation of data directory [%s] cannot be read: %s", dataDirectory,
                    Throwables.concatMessages(t));
            return false;
        }
    }

    private static void addSummaryNames(final Set<String> names, final List<MemoryFileSummary> summaries) {
        for (final MemoryFileSummary summary : summaries) {
            names.add(new File(summary.getMemoryResourceUri()).getName());
        }
    }

    /**
     * True when the directory holds attempt local data files that are not the incomplete marker of an interrupted
     * write - i.e. bytes a publish is supposed to have named somewhere.
     */
    private static boolean hasPublishedBytesWithoutIndex(final File dataDirectory) {
        for (final File attemptDataFile : listAttemptDataFiles(dataDirectory, null)) {
            if (!attemptDataFile.getName().contains(MemoryFiles.INCOMPLETE_MEMORY_FILE_MARKER)) {
                return true;
            }
        }
        return false;
    }


    /**
     * The published name set of one data directory plus whether reading it produced the complete picture. A sweep may
     * delete only on a complete picture; on an incomplete one it leaves the directory alone and the next completed
     * attempt re-submits it, which delays reclaim without risking payload.
     */
    private static final class ReferenceSet {

        private final Set<String> names;
        private final boolean reliable;
        private final String unreliableReason;
        private final boolean generationsPresent;
        private final List<File> prunableGenerations;

        private ReferenceSet(final Set<String> names, final boolean reliable, final String unreliableReason,
                final boolean generationsPresent, final List<File> prunableGenerations) {
            this.names = names;
            this.reliable = reliable;
            this.unreliableReason = unreliableReason;
            this.generationsPresent = generationsPresent;
            this.prunableGenerations = prunableGenerations;
        }

        private static ReferenceSet reliable(final Set<String> names, final List<File> prunableGenerations) {
            return new ReferenceSet(names, true, null, true, prunableGenerations);
        }

        private static ReferenceSet unreliable(final Set<String> names, final String unreliableReason,
                final boolean generationsPresent, final List<File> prunableGenerations) {
            return new ReferenceSet(names, false, unreliableReason, generationsPresent, prunableGenerations);
        }

        private Set<String> getNames() {
            return names;
        }

        private boolean isReliable() {
            return reliable;
        }

        private String getUnreliableReason() {
            return unreliableReason;
        }

        /**
         * Whether the directory had an index generation to read at all. It separates "this key never published" (its
         * own staged files are garbage and may go) from "an index exists but cannot be trusted" (nothing may go).
         */
        private boolean isGenerationsPresent() {
            return generationsPresent;
        }

        /**
         * The generations the walk proved redundant: every data file they name is named by a generation the walk kept,
         * and none of them is young enough to be an in-flight publish. Only a reliable set may have these deleted.
         */
        private List<File> getPrunableGenerations() {
            return prunableGenerations;
        }
    }

    /**
     * Reads one published index generation ('&lt;N&gt;_memory.index') of this key, or null when it cannot be read.
     * Null is deliberately not an empty list: the caller's reference set decides which payload files are garbage, so
     * "could not read" must not be reported as "names nothing" - that reading would delete the files this generation
     * was the last remaining record of. The serde parameters are captured as locals before the constructor runs:
     * SerializingCollection calls newSerde() virtually from its own constructor, so a subclass field would still be
     * null there.
     */
    private static List<MemoryFileSummary> readIndexGenerationSummaries(final Integer valueFixedLength,
            final ICompressionFactory compressionFactory, final File generationFile) {
        final List<MemoryFileSummary> summaries = new ArrayList<>();
        try {
            try (AMemoryFileSummarySerializingCollection collection = new AMemoryFileSummarySerializingCollection(
                    new TextDescription("%s.readIndexGenerationSummaries",
                            TimeSeriesEpochReclaim.class.getSimpleName()),
                    AtomicNioFileChannel.newFile(generationFile.toURI()), true) {

                @Override
                protected MemoryFileSummarySerde newSerde() {
                    return new MemoryFileSummarySerde(valueFixedLength);
                }

                @Override
                protected ICompressionFactory getCompressionFactory() {
                    return compressionFactory;
                }
            };
                    ICloseableIterator<MemoryFileSummary> iterator = collection.iterator()) {
                Lists.toListWithoutHasNext(iterator, summaries);
            }
            return summaries;
        } catch (final Throwable t) {
            //Not an empty list: see the javadoc - the caller treats an unreadable generation as an incomplete
            //reference set and leaves the directory alone, instead of deleting what it could not see.
            return null;
        }
    }

    /**
     * True while this key's marker describes a running attempt that began no later than the given attempt id was
     * minted AND that attempt's heartbeat claim is still fresh: an attempt mints its data file id only after it
     * started, so such a candidate may be the in-flight attempt's own bytes and must stay. The marker and its
     * heartbeat claim are read once per sweep (the progress argument), not once per candidate.
     */
    private static boolean isAttemptLive(final PendingReclaim pending, final TimeSeriesUpdaterProgress progress,
            final String attemptId) {
        if (progress == null || progress.getUpdateStart() == null) {
            return false;
        }
        final long createdMillis = attemptCreatedMillis(attemptId);
        if (createdMillis < progress.getUpdateStart().millisValue()) {
            //predates the attempt the current marker describes: its files are garbage, not in-flight bytes
            return false;
        }
        return isHeartbeatLive(progress.getOwner(),
                new File(pending.getUpdateProgressFile().getAbsolutePath() + ".lock"));
    }

    /**
     * Heartbeat liveness of a claimed update lock: the '.heartbeat' sibling of the lock file (falling back to the lock
     * file itself) must carry the marker owner and be touched within HEARTBEAT_TIMEOUT.
     */
    private static boolean isHeartbeatLive(final String progressOwner, final File updateLockFile) {
        final File heartbeatFile = Files.setExtensionNormalizeFileName(updateLockFile,
                HeartbeatFileChannelLockRegistry.HEARTBEAT_EXTENSION);
        final File claimFile = heartbeatFile.exists() ? heartbeatFile : updateLockFile;
        if (!claimFile.exists()) {
            return false;
        }
        final String claimOwner = Files.readFileToStringNoThrow(claimFile, Charsets.defaultCharset());
        if (claimOwner != null && !claimOwner.trim().equals(progressOwner)) {
            //the lock moved to another owner since the marker was written: this marker's attempt is not the live one
            return false;
        }
        return !HeartbeatFileChannelLockRegistry.HEARTBEAT_TIMEOUT.isLessThanMillis(
                FDateMillis.nowMillis() - claimFile.lastModified());
    }

    /**
     * The age of an attempt's garbage: the creation time embedded in its id ('&lt;millis&gt;&lt;suffix&gt;'), falling
     * back to the newest of its data file mtimes for ids that do not parse (then it is treated as young, which keeps
     * the sweep conservative).
     */
    private static long attemptAgeMillis(final String attemptId, final File[] attemptDataFiles) {
        long referenceMillis = attemptCreatedMillis(attemptId);
        if (referenceMillis <= 0) {
            for (final File attemptDataFile : attemptDataFiles) {
                referenceMillis = Longs.max(referenceMillis, attemptDataFile.lastModified());
            }
        }
        return FDateMillis.nowMillis() - referenceMillis;
    }

    /**
     * The attempt-start millis carried in an attempt id, or -1 when it does not parse. Attempt ids are monotonised per
     * JVM (see MemoryFiles.newEpochLocalDataId), so the embedded millis are the cheapest honest age reference - no
     * clock games.
     */
    private static long attemptCreatedMillis(final String attemptId) {
        if (attemptId == null) {
            return -1;
        }
        final int dash = attemptId.indexOf('-');
        final String millisPart = dash > 0 ? attemptId.substring(0, dash) : attemptId;
        try {
            return Long.parseLong(millisPart);
        } catch (final NumberFormatException e) {
            return -1;
        }
    }

    /**
     * Deletes the attempt's data files quietly (they may already be gone or held open on Windows; the next sweep or the
     * version-directory janitor finishes the job), unless a published generation still names them.
     */
    private static void deleteAttemptArtifacts(final File[] attemptDataFiles, final Set<String> referencedNames) {
        if (attemptDataFiles.length == 0) {
            //no data files of this attempt left (or the directory is already gone)
            return;
        }
        //per file and not per attempt: an attempt that published boundaries before it died keeps exactly the files a
        //published generation names, the files it staged after its last publish are unreferenced garbage and go
        for (final File dataFile : attemptDataFiles) {
            if (!referencedNames.contains(dataFile.getName())) {
                Files.deleteQuietly(dataFile);
            }
        }
    }

    /**
     * A submitted sweep: what the caller's cache pointed at, captured on the caller's thread.
     */
    private static final class PendingReclaim {
        private final String hashKey;
        private final File dataDirectory;
        private final File updateProgressFile;
        private final Integer valueFixedLength;
        private final ICompressionFactory compressionFactory;

        private PendingReclaim(final String hashKey, final File dataDirectory, final File updateProgressFile,
                final Integer valueFixedLength, final ICompressionFactory compressionFactory) {
            this.hashKey = hashKey;
            this.dataDirectory = dataDirectory;
            this.updateProgressFile = updateProgressFile;
            this.valueFixedLength = valueFixedLength;
            this.compressionFactory = compressionFactory;
        }

        private File getDataDirectory() {
            return dataDirectory;
        }

        private String getHashKey() {
            return hashKey;
        }

        private File getUpdateProgressFile() {
            return updateProgressFile;
        }

        private Integer getValueFixedLength() {
            return valueFixedLength;
        }

        private ICompressionFactory getCompressionFactory() {
            return compressionFactory;
        }
    }

}
