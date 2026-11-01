package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannel;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummarySerde;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFiles;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.AMemoryFileSummarySerializingCollection;
import de.invesdwin.util.collections.Collections;
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
 * path: the attempt's own transaction was discarded, so its files are provably unpinned garbage (the published
 * generation reference check still guards the rare case where a commit landed inside a throwing close);</li>
 * <li>{@link #reclaimObsoleteEpochs(TimeSeriesLookupStorageCache)} piggybacks on the completion of an update attempt
 * for that key and sweeps that key's version directory only: attempt files referenced by no published index
 * generation, not owned by the heartbeat-live attempt this key's progress marker describes, and older than
 * {@link TimeSeriesProperties#RETAIN_OBSOLETE_EPOCHS_THRESHOLD} go. Whole obsolete version directories (with all the
 * attempt files inside) are already swept by the lease janitor's cleanupObsoleteVersions.</li>
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
     * Best-effort, never throws: delete the given attempt's own artifacts (this attempt just aborted). Kept safe by the
     * published-generation reference check, so a commit that landed before a throwing transaction close is not undone.
     */
    public static void reclaimAbortedAttemptEpoch(final TimeSeriesLookupStorageCache<?, ?> cache,
            final String attemptId) {
        if (attemptId == null) {
            //legacy shared-data attempt: nothing attempt local to reclaim
            return;
        }
        try {
            final File dataDirectory = cache.getDirectoryHashKeyVersionMemory()
                    .getDirectoryHashKeyVersionDataShared();
            deleteAttemptArtifacts(cache, dataDirectory, attemptId);
        } catch (final Throwable t) {
            //reclaim is an optimization, never a cause of failure
            LOG.warn("Best-effort reclaim of the aborted attempt [%s] of key [%s] failed: %s", attemptId,
                    cache.getHashKey(), Throwables.concatMessages(t));
        }
    }

    /**
     * Best-effort, never throws: sweep the dead attempt garbage of this key's current version directory. Called when an
     * update attempt for the key finishes (piggyback, no new timer). Candidates are the attempt local data file names
     * in that directory, so bytes orphaned when their generation was aged out are found too. A candidate goes only when
     * it is not the attempt the current marker describes with a fresh heartbeat claim, and is older than
     * {@link TimeSeriesProperties#RETAIN_OBSOLETE_EPOCHS_THRESHOLD}; its files stay while any retained published
     * generation names them.
     */
    public static void reclaimObsoleteEpochs(final TimeSeriesLookupStorageCache<?, ?> cache) {
        try {
            final File dataDirectory = cache.getDirectoryHashKeyVersionMemory()
                    .getDirectoryHashKeyVersionDataShared();
            final File[] attemptDataFiles = listAttemptDataFiles(dataDirectory, null);
            final List<String> candidateIds = new ArrayList<>();
            for (final File attemptDataFile : attemptDataFiles) {
                final String attemptId = attemptIdOfDataFileName(attemptDataFile.getName());
                if (attemptId != null && !candidateIds.contains(attemptId)) {
                    candidateIds.add(attemptId);
                }
            }
            for (final String attemptId : candidateIds) {
                if (isAttemptLive(cache, attemptId)) {
                    //owned by the heartbeat-live attempt this key's marker describes: never touch
                    continue;
                }
                final File[] dataFiles = listAttemptDataFiles(dataDirectory, attemptId);
                if (!TimeSeriesProperties.RETAIN_OBSOLETE_EPOCHS_THRESHOLD.isLessThanMillis(
                        attemptAgeMillis(attemptId, dataFiles))) {
                    //younger than the retention threshold: a reader may still be resolving it
                    continue;
                }
                deleteAttemptArtifacts(cache, dataFiles);
            }
        } catch (final Throwable t) {
            //reclaim is an optimization, never a cause of failure
            LOG.warn("Best-effort attempt reclaim sweep of key [%s] failed: %s", cache.getHashKey(),
                    Throwables.concatMessages(t));
        }
    }

    /**
     * The attempt id embedded in an attempt local data file name: 'memory_&lt;attemptId&gt;' up to the first dot (the
     * extensions are '.data', '.data.&lt;preceding offset&gt;' and '.data_incomplete_...'), null for names that do not
     * carry one (a legacy shared 'memory.data' never passes {@link MemoryFiles#isEpochLocalDataFile(String)} anyway).
     */
    private static String attemptIdOfDataFileName(final String fileName) {
        final int start = MemoryFiles.EPOCH_LOCAL_DATA_FILE_PREFIX.length();
        final int end = fileName.indexOf('.', start);
        return end > start ? fileName.substring(start, end) : null;
    }

    /**
     * The attempt local data files of the version's data directory, all of them when attemptId is null and only the
     * ones of that attempt otherwise (exact 'memory_&lt;attemptId&gt;' prefix match, which covers '.data',
     * '.data.&lt;offset&gt;' and the '_incomplete_' variants of that attempt and can never hit the shared
     * 'memory.data' or another attempt's files).
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
     * True when the attempt's files are still referenced as a data file source by any '&lt;N&gt;_memory.index'
     * generation that exists in the version's data directory (the newest published view plus the older generations
     * retained for open iterations). This is the trap M1 flagged: never delete a file an existing generation's
     * summaries name.
     */
    private static boolean isReferencedByPublishedGenerations(final TimeSeriesLookupStorageCache<?, ?> cache,
            final File[] attemptDataFiles) {
        final File dataDirectory = attemptDataFiles[0].getParentFile();
        final File[] generationFiles = dataDirectory.listFiles(
                (dir, name) -> name.endsWith("_" + AMemoryFileSummarySerializingCollection.MEMORY_INDEX_FILE_NAME));
        if (generationFiles == null) {
            return false;
        }
        for (final File generationFile : generationFiles) {
            final List<MemoryFileSummary> summaries = readIndexGenerationSummaries(cache, generationFile);
            for (final MemoryFileSummary summary : summaries) {
                final String dataFileName = new File(summary.getMemoryResourceUri()).getName();
                for (final File attemptDataFile : attemptDataFiles) {
                    if (attemptDataFile.getName().equals(dataFileName)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Reads one published index generation ('&lt;N&gt;_memory.index') of this key; a corrupt or concurrently replaced
     * generation yields no summaries, which only ever makes a reclaim sweep more conservative about that file. The
     * serde parameters are captured as locals before the constructor runs: SerializingCollection calls newSerde()
     * virtually from its own constructor, so a subclass field would still be null there.
     */
    private static List<MemoryFileSummary> readIndexGenerationSummaries(
            final TimeSeriesLookupStorageCache<?, ?> cache, final File generationFile) {
        final List<MemoryFileSummary> summaries = new ArrayList<>();
        try {
            final Integer valueFixedLength = cache.getValueFixedLength();
            final ICompressionFactory compressionFactory = cache.getCompressionFactory();
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
     * started, so such a candidate may be the in-flight attempt's own bytes and must stay. The sweep only ever runs
     * from an update attempt holding this key's pin, so no other attempt of this key can be in flight beside it.
     */
    private static boolean isAttemptLive(final TimeSeriesLookupStorageCache<?, ?> cache, final String attemptId) {
        final File updateProgressFile = cache.getUpdateProgressFile();
        final TimeSeriesUpdaterProgress progress = TimeSeriesUpdaterProgress.readUpdateProgress(updateProgressFile);
        if (progress == null || progress.getUpdateStart() == null) {
            return false;
        }
        final long createdMillis = attemptCreatedMillis(attemptId);
        if (createdMillis < progress.getUpdateStart().millisValue()) {
            //predates the attempt the current marker describes: its files are garbage, not in-flight bytes
            return false;
        }
        return isHeartbeatLive(progress.getOwner(), new File(updateProgressFile.getAbsolutePath() + ".lock"));
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
    private static void deleteAttemptArtifacts(final TimeSeriesLookupStorageCache<?, ?> cache,
            final File dataDirectory, final String attemptId) {
        deleteAttemptArtifacts(cache, listAttemptDataFiles(dataDirectory, attemptId));
    }

    private static void deleteAttemptArtifacts(final TimeSeriesLookupStorageCache<?, ?> cache,
            final File[] attemptDataFiles) {
        if (attemptDataFiles.length == 0) {
            //no data files of this attempt left (or the directory is already gone)
            return;
        }
        if (isReferencedByPublishedGenerations(cache, attemptDataFiles)) {
            //a published generation names this attempt's files: the bytes must stay (M1 trap: deleting files the
            //published generation references is data loss)
            return;
        }
        for (final File dataFile : attemptDataFiles) {
            Files.deleteQuietly(dataFile);
        }
    }

}
