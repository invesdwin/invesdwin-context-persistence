package de.invesdwin.context.persistence.timeseriesdb.storage.memory;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.filechannel.IFileChannel;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdaterInternalMethods;
import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLockRegistry;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.OperatingSystem;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.millis.FDateMillis;

@Immutable
public final class MemoryFiles {

    /**
     * Prefix of the data files that belong to a single update attempt (epoch local layout - the only layout of this
     * store).
     */
    public static final String EPOCH_LOCAL_DATA_FILE_PREFIX = "memory_";
    /**
     * Marks the standalone file an attempt writes for its incomplete last segment; it sits inside the attempt local
     * data file name, so an attempt id ends at it.
     */
    public static final String INCOMPLETE_MEMORY_FILE_MARKER = "_incomplete_";
    private static final String MEMORY_DATA_FILE_EXTENSION = ".data";
    private static final int EPOCH_LOCAL_DATA_ID_TOKEN_LENGTH = 8;
    private static final String EPOCH_LOCAL_DATA_ID_OWNER_TOKEN = newEpochLocalDataIdOwnerToken();
    private static final AtomicLong LAST_EPOCH_LOCAL_DATA_ID_MILLIS = new AtomicLong();

    private MemoryFiles() {}

    /**
     * Identifies the data files of one update attempt: the creation millis (monotonised per JVM so that two attempts
     * started in the same millisecond still get distinct files) followed by a short per-JVM token, which makes the ids
     * sortable and unique across JVMs.
     */
    public static String newEpochLocalDataId() {
        long millis = FDateMillis.nowMillis();
        long previousMillis;
        do {
            previousMillis = LAST_EPOCH_LOCAL_DATA_ID_MILLIS.get();
            if (millis <= previousMillis) {
                millis = previousMillis + 1L;
            }
        } while (!LAST_EPOCH_LOCAL_DATA_ID_MILLIS.compareAndSet(previousMillis, millis));
        return millis + "-" + EPOCH_LOCAL_DATA_ID_OWNER_TOKEN;
    }

    private static String newEpochLocalDataIdOwnerToken() {
        //HeartbeatFileChannelLockRegistry appends a pseudo random UUID to the JVM name exactly for per-JVM uniqueness, a short prefix of it keeps the data file names small
        final String owner = HeartbeatFileChannelLockRegistry.HEARTBEAT_OWNER;
        final int uuidStart = owner.lastIndexOf('_');
        final String uuidPart = uuidStart > -1 ? owner.substring(uuidStart + 1) : owner;
        return Files.normalizeFileName(Strings.truncate(uuidPart, EPOCH_LOCAL_DATA_ID_TOKEN_LENGTH));
    }

    public static boolean isEpochLocalDataFile(final String memoryFileName) {
        //the id part always starts with the millis, which keeps names without an attempt id (such as an older
        //'memory_incomplete_...' file) out of this check
        return memoryFileName.length() > EPOCH_LOCAL_DATA_FILE_PREFIX.length()
                && memoryFileName.startsWith(EPOCH_LOCAL_DATA_FILE_PREFIX)
                && Character.isDigit(memoryFileName.charAt(EPOCH_LOCAL_DATA_FILE_PREFIX.length()));
    }

    public static File newIncompleteMemoryFile(final File memoryFile, final long memoryOffset) {
        final String timestampStr = FDate.now().toString(FDate.FORMAT_NUMBER_DATE_TIME_PS);
        final String extension = Files.getExtension(memoryFile);
        final String incompleteFilename = Files.removeExtension(memoryFile.getName()) + INCOMPLETE_MEMORY_FILE_MARKER
                + memoryOffset + "_" + timestampStr + extension;
        //an attempt local write leaves its own incomplete files for the reclaim sweep (TimeSeriesEpochReclaim) instead
        //of deleting files that another attempt may have published
        return new File(memoryFile.getParentFile(), incompleteFilename);
    }

    public static boolean isIncompleteMemoryFile(final IFileChannel memoryFile) {
        return isIncompleteMemoryFile(memoryFile.getFileName());
    }

    public static boolean isIncompleteMemoryFile(final String memoryFileName) {
        return memoryFileName.contains(INCOMPLETE_MEMORY_FILE_MARKER);
    }

    /**
     * The data file this attempt's next chunk is written to; always named after the attempt (epoch local layout - the
     * only layout this store has).
     */
    public static File newMemoryFile(final ITimeSeriesUpdaterInternalMethods<?, ?> parent,
            final long precedingMemoryOffset) {
        final String epochLocalDataId = parent.getEpochLocalDataId();
        if (Strings.isBlank(epochLocalDataId)) {
            //an attempt without an id of its own would append to data files another attempt may have published, so the
            //non-null contract of getEpochLocalDataId is refused here before any file is resolved
            throw new IllegalStateException(
                    "getEpochLocalDataId() of [" + parent.getClass().getName() + "] returned no epoch local data id"
                            + " (key=" + parent.getKey() + "): the epoch local data file layout is the only supported"
                            + " layout, every update attempt must write data files named after itself");
        }
        final File dataDirectory = parent.getLookupTable().getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        return newEpochLocalMemoryFile(dataDirectory, epochLocalDataId, precedingMemoryOffset);
    }

    /**
     * Data files of an attempt local write are named after that attempt and are written once and never modified
     * afterwards: a redo cannot reach bytes that the currently published index still references (it gets files of its
     * own), and an abandoned attempt leaves nothing but garbage behind. Further chunks of the same attempt are suffixed
     * with the bytes their preceding chunks wrote, the same offset suffix scheme that a chunked write uses on Windows,
     * so the progress writers need no extra chunk counter.
     */
    private static File newEpochLocalMemoryFile(final File dataDirectory, final String epochLocalDataId,
            final long precedingMemoryOffset) {
        final StringBuilder fileName = new StringBuilder(EPOCH_LOCAL_DATA_FILE_PREFIX).append(epochLocalDataId)
                .append(MEMORY_DATA_FILE_EXTENSION);
        if (precedingMemoryOffset > 0L) {
            fileName.append('.').append(precedingMemoryOffset);
        }
        final File memoryFile = new File(dataDirectory, fileName.toString());
        if (memoryFile.exists()) {
            throw new IllegalStateException(
                    "Attempt local data file already exists, refusing to write into bytes another update attempt may have published: "
                            + memoryFile);
        }
        return memoryFile;
    }

    /**
     * Not the epoch local contract: this one is for callers that name their own data file outside a version directory
     * (a store type that never had update attempts, such as the live calculated segment files) and only need the
     * Windows chunk suffix applied.
     */
    public static File newMemoryFile(final File memoryFile, final long precedingMemoryOffset) {
        if (OperatingSystem.isWindows()) {
            return new File(memoryFile.getAbsolutePath() + "." + precedingMemoryOffset);
        } else {
            return memoryFile;
        }
    }

    public static void assertMaybeFirstSummary(final long precedingMemoryOffset, final long memoryOffset,
            final long precedingValueCount) {
        if (precedingMemoryOffset == 0 && memoryOffset == 0) {
            if (precedingValueCount != 0) {
                throw new IllegalArgumentException(
                        "If precedingMemoryOffset and memoryOffset are 0, then precedingValueCount must also be 0: precedingMemoryOffset="
                                + precedingMemoryOffset + ", memoryOffset=" + memoryOffset + ", precedingValueCount="
                                + precedingValueCount);
            }
        }
        if (precedingValueCount == 0) {
            if (precedingMemoryOffset != 0) {
                throw new IllegalArgumentException(
                        "If precedingValueCount is 0, then precedingMemoryOffset must also be 0: precedingMemoryOffset="
                                + precedingMemoryOffset + ", memoryOffset=" + memoryOffset + ", precedingValueCount="
                                + precedingValueCount);
            }
            if (memoryOffset != 0) {
                throw new IllegalArgumentException(
                        "If precedingValueCount is 0, then memoryOffset must also be 0: precedingMemoryOffset="
                                + precedingMemoryOffset + ", memoryOffset=" + memoryOffset + ", precedingValueCount="
                                + precedingValueCount);
            }
        }
    }

    public static void assertFirstSummary(final long precedingMemoryOffset, final long memoryOffset,
            final long precedingValueCount) {
        if (precedingMemoryOffset != 0) {
            throw new IllegalArgumentException(
                    "If precedingMemoryOffset is not 0, then this is not the first summary: precedingMemoryOffset="
                            + precedingMemoryOffset + ", memoryOffset=" + memoryOffset + ", precedingValueCount="
                            + precedingValueCount);
        }
        if (memoryOffset != 0) {
            throw new IllegalArgumentException(
                    "If memoryOffset is not 0, then this is not the first summary: precedingMemoryOffset="
                            + precedingMemoryOffset + ", memoryOffset=" + memoryOffset + ", precedingValueCount="
                            + precedingValueCount);
        }
        if (precedingValueCount != 0) {
            throw new IllegalArgumentException(
                    "If precedingValueCount is not 0, then this is not the first summary: precedingMemoryOffset="
                            + precedingMemoryOffset + ", memoryOffset=" + memoryOffset + ", precedingValueCount="
                            + precedingValueCount);
        }
    }

}