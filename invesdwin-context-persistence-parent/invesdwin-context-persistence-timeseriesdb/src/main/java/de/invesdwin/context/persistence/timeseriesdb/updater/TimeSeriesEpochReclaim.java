package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFiles;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.AMemoryFileSummarySerializingCollection;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLockRegistry;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.string.Charsets;
import de.invesdwin.util.time.date.millis.FDateMillis;

/**
 * Epoch reclaim (epoch design M3-cp): every update attempt (epoch) leaves epoch local data files
 * ('memory_&lt;epochId&gt;.data[.&lt;offset&gt;]' including its '_incomplete_' variants) in the version's data directory
 * and a staged index snapshot under '&lt;versionDir&gt;/epoch-staging/&lt;epochId&gt;/' behind - M1 removed the
 * cross-attempt '_incomplete_' sweep because it deleted files the published generation references, so without this
 * reclaim every attempt accumulates garbage. Reclaim runs without a new global timer:
 * <ul>
 * <li>{@link #reclaimAbortedAttemptEpoch(TimeSeriesLookupStorageCache, String)} runs in the failing writer's abort
 * path: the attempt's own transaction was discarded, so its files are provably unpinned garbage (the published
 * generation reference check still guards the rare case where a commit landed inside a throwing close);</li>
 * <li>{@link #reclaimObsoleteEpochs(TimeSeriesLookupStorageCache)} piggybacks on the completion of an update attempt
 * for that key and sweeps that key's version directory only: epochs referenced by no published index generation, not
 * owned by a heartbeat-live progress marker, and older than
 * {@link TimeSeriesProperties#RETAIN_OBSOLETE_EPOCHS_THRESHOLD} go. Whole obsolete version directories (with all the
 * epochs inside) are already swept by the lease janitor's cleanupObsoleteVersions.</li>
 * </ul>
 * The reference set is collected from ALL '&lt;N&gt;_memory.index' generations still present on disk (not just the
 * newest): older generations are retained for the length of the longest backtest (RETAIN_OBSOLETE_FILES_THRESHOLD)
 * and open iterations resolve one of them, so a file named by any existing generation must stay readable.
 */
@ThreadSafe
public final class TimeSeriesEpochReclaim {

    private static final Log LOG = new Log(TimeSeriesEpochReclaim.class);

    private TimeSeriesEpochReclaim() {}

    /**
     * Best-effort, never throws: delete the given epoch's own artifacts (this attempt just aborted). Kept safe by the
     * published-generation reference check, so a commit that landed before a throwing transaction close is not undone.
     */
    public static void reclaimAbortedAttemptEpoch(final TimeSeriesLookupStorageCache<?, ?> cache,
            final String epochId) {
        if (epochId == null) {
            //legacy shared-data attempt: nothing epoch local to reclaim
            return;
        }
        try {
            final File versionDirectory = getVersionDirectory(cache);
            if (!versionDirectory.isDirectory()) {
                return;
            }
            final File dataDirectory = cache.getDirectoryHashKeyVersionMemory()
                    .getDirectoryHashKeyVersionDataShared();
            deleteEpochArtifacts(cache, versionDirectory, dataDirectory, epochId);
        } catch (final Throwable t) {
            //reclaim is an optimization, never a cause of failure
            LOG.warn("Best-effort reclaim of the aborted attempt epoch [%s] of key [%s] failed: %s", epochId,
                    cache.getHashKey(), Throwables.concatMessages(t));
        }
    }

    /**
     * Best-effort, never throws: sweep the dead epoch garbage of this key's current version directory. Called when an
     * update attempt for the key finishes (piggyback, no new timer). Candidate epochs come from both the staging
     * directory names and the epoch local data file names, so bytes orphaned when their generation was aged out are
     * found too. A candidate goes only when it is not heartbeat-live, older than
     * {@link TimeSeriesProperties#RETAIN_OBSOLETE_EPOCHS_THRESHOLD}, and its data files are kept while any retained
     * published generation names them (deleteEpochArtifacts splits staging metadata from referenced bytes).
     */
    public static void reclaimObsoleteEpochs(final TimeSeriesLookupStorageCache<?, ?> cache) {
        try {
            final File versionDirectory = getVersionDirectory(cache);
            final File stagingRoot = new File(versionDirectory,
                    TimeSeriesUpdateTransaction.EPOCH_STAGING_DIRECTORY_NAME);
            final File dataDirectory = cache.getDirectoryHashKeyVersionMemory()
                    .getDirectoryHashKeyVersionDataShared();
            final Set<String> candidateEpochs = ILockCollectionFactory.getInstance(false).newLinkedSet();
            final File[] stagingDirectories = stagingRoot.isDirectory() ? stagingRoot
                    .listFiles(File::isDirectory) : null;
            if (stagingDirectories != null) {
                for (final File stagingDirectory : stagingDirectories) {
                    candidateEpochs.add(stagingDirectory.getName());
                }
            }
            final File[] epochDataFiles = dataDirectory.listFiles(
                    (dir, name) -> MemoryFiles.isEpochLocalDataFile(name));
            if (epochDataFiles != null) {
                for (final File epochDataFile : epochDataFiles) {
                    final String epochId = epochIdOfDataFileName(epochDataFile.getName());
                    if (epochId != null) {
                        candidateEpochs.add(epochId);
                    }
                }
            }
            final File updateProgressFile = cache.getUpdateProgressFile();
            for (final String epochId : candidateEpochs) {
                if (isEpochLive(updateProgressFile, epochId)) {
                    //owned by the heartbeat-live marker of an attempt that is still running: never touch
                    continue;
                }
                if (!TimeSeriesProperties.RETAIN_OBSOLETE_EPOCHS_THRESHOLD.isLessThanMillis(
                        epochAgeMillis(epochId, stagingRoot))) {
                    //younger than the retention threshold: a demand may still be reading its staged snapshot
                    continue;
                }
                deleteEpochArtifacts(cache, versionDirectory, dataDirectory, epochId);
            }
        } catch (final Throwable t) {
            //reclaim is an optimization, never a cause of failure
            LOG.warn("Best-effort epoch reclaim sweep of key [%s] failed: %s", cache.getHashKey(),
                    Throwables.concatMessages(t));
        }
    }

    /**
     * The epoch id embedded in an epoch local data file name: 'memory_&lt;epochId&gt;' up to the first dot (the
     * extensions are '.data', '.data.&lt;preceding offset&gt;' and '.data_incomplete_...'), null for names that do not
     * carry one (a legacy shared 'memory.data' never passes {@link MemoryFiles#isEpochLocalDataFile(String)} anyway).
     */
    private static String epochIdOfDataFileName(final String fileName) {
        final int start = MemoryFiles.EPOCH_LOCAL_DATA_FILE_PREFIX.length();
        final int end = fileName.indexOf('.', start);
        return end > start ? fileName.substring(start, end) : null;
    }

    /**
     * True when the epoch is still referenced as a data file source by any '&lt;N&gt;_memory.index' generation that
     * exists in the version's data directory (the newest published view plus the older generations retained for open
     * iterations). This is the trap M1 flagged: never delete a file an existing generation's summaries name.
     */
    public static boolean isEpochReferencedByPublishedGenerations(final TimeSeriesLookupStorageCache<?, ?> cache,
            final File dataDirectory, final String epochId) {
        final File[] generationFiles = dataDirectory.listFiles(
                (dir, name) -> name.endsWith("_" + AMemoryFileSummarySerializingCollection.MEMORY_INDEX_FILE_NAME));
        if (generationFiles == null) {
            return false;
        }
        final String epochDataFilePrefix = MemoryFiles.EPOCH_LOCAL_DATA_FILE_PREFIX + epochId;
        for (final File generationFile : generationFiles) {
            final List<MemoryFileSummary> summaries = readIndexGenerationSummaries(cache, generationFile);
            for (final MemoryFileSummary summary : summaries) {
                final String dataFileName = new File(summary.getMemoryResourceUri()).getName();
                if (dataFileName.startsWith(epochDataFilePrefix)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Reads one published index generation ('&lt;N&gt;_memory.index') of this key; a corrupt or concurrently replaced
     * generation yields no summaries, which only ever makes a reclaim sweep more conservative about that file.
     */
    public static List<MemoryFileSummary> readIndexGenerationSummaries(final TimeSeriesLookupStorageCache<?, ?> cache,
            final File generationFile) {
        try {
            final List<MemoryFileSummary> summaries = new ArrayList<>();
            try (ICloseableIterator<MemoryFileSummary> iterator = TimeSeriesUpdateTransaction
                    .readSummaryCollection(cache, generationFile).iterator()) {
                Lists.toListWithoutHasNext(iterator, summaries);
            }
            return summaries;
        } catch (final Throwable t) {
            return Collections.emptyList();
        }
    }

    /**
     * True when the current progress marker of the key names the epoch AND the marker's heartbeat claim is still
     * fresh: the attempt is still running (or died within the heartbeat timeout), so its files and snapshot stay.
     */
    public static boolean isEpochLive(final File updateProgressFile, final String epochId) {
        final TimeSeriesUpdaterProgress progress = TimeSeriesUpdaterProgress.readUpdateProgress(updateProgressFile);
        if (progress == null || !Objects.equals(progress.getEpochId(), epochId)) {
            return false;
        }
        return isEpochOwnerLive(progress.getOwner(), new File(updateProgressFile.getAbsolutePath() + ".lock"));
    }

    /**
     * Heartbeat liveness of a claimed update lock: the '.heartbeat' sibling of the lock file (falling back to the lock
     * file itself) must carry the marker owner and be touched within HEARTBEAT_TIMEOUT. Shared with the M2 adoption
     * gate in {@link ATimeSeriesUpdater}.
     */
    static boolean isEpochOwnerLive(final String progressOwner, final File updateLockFile) {
        final File heartbeatFile = Files.setExtensionNormalizeFileName(updateLockFile,
                HeartbeatFileChannelLockRegistry.HEARTBEAT_EXTENSION);
        final File claimFile = heartbeatFile.exists() ? heartbeatFile : updateLockFile;
        if (!claimFile.exists()) {
            return false;
        }
        final String claimOwner = Files.readFileToStringNoThrow(claimFile, Charsets.defaultCharset());
        if (claimOwner != null && !claimOwner.trim().equals(progressOwner)) {
            //the lock moved to another owner since the marker was written: this marker's epoch is not the live one
            return false;
        }
        return !HeartbeatFileChannelLockRegistry.HEARTBEAT_TIMEOUT.isLessThanMillis(
                FDateMillis.nowMillis() - claimFile.lastModified());
    }

    /**
     * The creation time embedded in the epoch id ('&lt;millis&gt;-&lt;ownerToken&gt;'), falling back to the staging
     * directory's mtime for ids that do not parse.
     */
    private static long epochAgeMillis(final String epochId, final File stagingDirectory) {
        final long createdMillis = epochCreatedMillis(epochId);
        final long referenceMillis = createdMillis > 0 ? createdMillis : stagingDirectory.lastModified();
        return FDateMillis.nowMillis() - referenceMillis;
    }

    /**
     * The attempt-start millis carried in an epoch id, or -1 when it does not parse. Epoch ids are monotonised per JVM
     * (see MemoryFiles.newEpochLocalDataId), so the embedded millis are the cheapest honest age reference - no extra
     * stat and no clock games.
     */
    public static long epochCreatedMillis(final String epochId) {
        if (epochId == null) {
            return -1;
        }
        final int dash = epochId.indexOf('-');
        final String millisPart = dash > 0 ? epochId.substring(0, dash) : epochId;
        try {
            return Long.parseLong(millisPart);
        } catch (final NumberFormatException e) {
            return -1;
        }
    }

    private static File getVersionDirectory(final TimeSeriesLookupStorageCache<?, ?> cache) {
        return cache.getDirectoryHashKey().getDirectoryHashKeyVersion().getDirectoryHashKeyVersionShared();
    }

    /**
     * Deletes the staging directory and every data file of the epoch, all quietly (files may already be gone or held
     * open on Windows; the next sweep or the version-directory janitor finishes the job). Data files are matched by
     * the exact 'memory_&lt;epochId&gt;' name prefix, which covers '.data', '.data.&lt;offset&gt;' and the
     * '_incomplete_' variants of that attempt and can never hit the shared 'memory.data' or another epoch's files.
     */
    private static void deleteEpochArtifacts(final TimeSeriesLookupStorageCache<?, ?> cache,
            final File versionDirectory, final File dataDirectory, final String epochId) {
        final File stagingDirectory = new File(
                new File(versionDirectory, TimeSeriesUpdateTransaction.EPOCH_STAGING_DIRECTORY_NAME), epochId);
        final File[] dataFiles = dataDirectory.listFiles(
                (dir, name) -> name.startsWith(MemoryFiles.EPOCH_LOCAL_DATA_FILE_PREFIX + epochId));
        if (dataFiles == null || dataFiles.length == 0) {
            //no data files of this epoch (or the directory is already gone): drop just the staging metadata
            Files.deleteQuietly(stagingDirectory);
            return;
        }
        if (isEpochReferencedByPublishedGenerations(cache, dataDirectory, epochId)) {
            //a published generation names this epoch's files: the bytes must stay (M1 trap: deleting files the
            //published generation references is data loss); the staging snapshot is unreferenced metadata, delete it
            Files.deleteQuietly(stagingDirectory);
            return;
        }
        for (final File dataFile : dataFiles) {
            Files.deleteQuietly(dataFile);
        }
        Files.deleteQuietly(stagingDirectory);
    }

}
