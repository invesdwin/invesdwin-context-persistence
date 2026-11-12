package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
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
import de.invesdwin.util.collections.Collections;
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
 */
@ThreadSafe
public final class TimeSeriesEpochReclaim {

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
            deleteAttemptArtifacts(listAttemptDataFiles(dataDirectory, attemptId),
                    collectPublishedDataFileNames(dataDirectory, cache.getValueFixedLength(),
                            cache.getCompressionFactory()));
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
            //ONE listing for the whole sweep: the attempt local files bucketed by the attempt id they carry. Listing
            //per attempt made the sweep quadratic in the directory's file count (measured: 2,992 entries in one live
            //key's directory).
            final Map<String, List<File>> filesByAttemptId = ILockCollectionFactory.getInstance(false).newLinkedMap();
            for (final File attemptDataFile : listAttemptDataFiles(dataDirectory, null)) {
                final String attemptId = attemptIdOfDataFileName(attemptDataFile.getName());
                if (attemptId != null) {
                    filesByAttemptId.computeIfAbsent(attemptId, k -> new ArrayList<>()).add(attemptDataFile);
                }
            }
            if (filesByAttemptId.isEmpty()) {
                return;
            }
            //ONE reference set for the whole sweep, built lazily: it depends on the directory, not on the attempt, and
            //reading it decompresses every retained index generation of the key.
            Set<String> referencedNames = null;
            //ONE read of this key's progress marker and heartbeat claim for the same reason.
            final TimeSeriesUpdaterProgress progress = TimeSeriesUpdaterProgress.readUpdateProgress(
                    pending.getUpdateProgressFile());
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
                    referencedNames = collectPublishedDataFileNames(dataDirectory, pending.getValueFixedLength(),
                            pending.getCompressionFactory());
                }
                deleteAttemptArtifacts(dataFiles, referencedNames);
            }
        } catch (final Throwable t) {
            //reclaim is an optimization, never a cause of failure
            LOG.warn("Best-effort attempt reclaim sweep of key [%s] failed: %s", pending.getHashKey(),
                    Throwables.concatMessages(t));
        }
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
     * garbage.
     */
    private static Set<String> collectPublishedDataFileNames(final File dataDirectory,
            final Integer valueFixedLength, final ICompressionFactory compressionFactory) {
        final Set<String> referencedNames = ILockCollectionFactory.getInstance(true).newLinkedSet();
        final File[] generationFiles = dataDirectory.listFiles(
                (dir, name) -> name.endsWith("_" + AMemoryFileSummarySerializingCollection.MEMORY_INDEX_FILE_NAME));
        if (generationFiles == null) {
            return referencedNames;
        }
        for (final File generationFile : generationFiles) {
            final List<MemoryFileSummary> summaries = readIndexGenerationSummaries(valueFixedLength, compressionFactory,
                    generationFile);
            for (final MemoryFileSummary summary : summaries) {
                referencedNames.add(new File(summary.getMemoryResourceUri()).getName());
            }
        }
        return referencedNames;
    }

    /**
     * Reads one published index generation ('&lt;N&gt;_memory.index') of this key; a corrupt or concurrently replaced
     * generation yields no summaries, which only ever makes a reclaim sweep more conservative about that file. The
     * serde parameters are captured as locals before the constructor runs: SerializingCollection calls newSerde()
     * virtually from its own constructor, so a subclass field would still be null there.
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
            return Collections.emptyList();
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
