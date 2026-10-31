package de.invesdwin.context.persistence.timeseriesdb.storage.memory;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.filechannel.IFileChannel;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdaterInternalMethods;
import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLockRegistry;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.OperatingSystem;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.date.millis.FDateMillis;

@Immutable
public final class MemoryFiles {

    /**
     * The shared data file of a version directory (legacy layout, one file appended to by every update attempt).
     */
    public static final String MEMORY_DATA_FILE_NAME = "memory.data";
    /**
     * Prefix of the data files that belong to a single update attempt (epoch local layout).
     */
    public static final String EPOCH_LOCAL_DATA_FILE_PREFIX = "memory_";
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
        //the id part always starts with the millis, which also keeps legacy names like 'memory.data' or 'memory_incomplete_...' out of this check
        return memoryFileName.length() > EPOCH_LOCAL_DATA_FILE_PREFIX.length()
                && memoryFileName.startsWith(EPOCH_LOCAL_DATA_FILE_PREFIX)
                && Character.isDigit(memoryFileName.charAt(EPOCH_LOCAL_DATA_FILE_PREFIX.length()));
    }

    public static File newIncompleteMemoryFile(final File memoryFile, final long memoryOffset) {
        final String timestampStr = FDate.now().toString(FDate.FORMAT_NUMBER_DATE_TIME_PS);
        final String extension = Files.getExtension(memoryFile);
        final String incompleteFilename = Files.removeExtension(memoryFile.getName()) + "_incomplete_" + memoryOffset
                + "_" + timestampStr + extension;
        final File incompleteFile = new File(memoryFile.getParentFile(), incompleteFilename);
        if (!isEpochLocalDataFile(memoryFile.getName())) {
            //the shared layout reuses one file per version directory, so this is the only place where its stale incomplete files are kept bounded; an epoch local attempt leaves its own files for the epoch reclaim instead of deleting files that another attempt may have published
            cleanUpOldIncompleteFiles(memoryFile);
        }
        return incompleteFile;
    }

    public static boolean isIncompleteMemoryFile(final IFileChannel memoryFile) {
        return isIncompleteMemoryFile(memoryFile.getFileName());
    }

    public static boolean isIncompleteMemoryFile(final String memoryFileName) {
        return memoryFileName.contains("_incomplete_");
    }

    public static void cleanUpOldIncompleteFiles(final File memoryFile) {
        final File parentFile = memoryFile.getParentFile();
        if (parentFile == null || !parentFile.exists()) {
            return;
        }

        final File[] files = parentFile.listFiles((dir, name) -> isIncompleteMemoryFile(name));
        if (files == null || files.length == 0) {
            return;
        }

        final FDate[] fileDates = new FDate[files.length];

        File latestFile = null;
        FDate latestDate = FDates.MIN_DATE;

        for (int i = 0; i < files.length; i++) {
            final File f = files[i];
            final String name = f.getName();
            final String noExt = Files.removeExtension(name);
            final int lastUnderscore = noExt.lastIndexOf('_');
            if (lastUnderscore != -1) {
                final String timestampStr = noExt.substring(lastUnderscore + 1);
                final FDate date = FDate.valueOf(timestampStr, FDate.FORMAT_NUMBER_DATE_TIME_PS);
                fileDates[i] = date;
                if (date.isAfterNotNullSafe(latestDate)) {
                    latestDate = date;
                    latestFile = f;
                }
            }
        }

        for (int i = 0; i < files.length; i++) {
            final File f = files[i];
            if (f.equals(latestFile)) {
                continue;
            } else {
                final FDate date = fileDates[i];
                if (date == null) {
                    continue;
                }
                if (TimeSeriesProperties.RETAIN_OBSOLETE_FILES_THRESHOLD.isGreaterThan(date, latestDate)) {
                    continue;
                }
            }
            Files.deleteQuietly(f);
        }
    }

    public static File newMemoryFile(final ITimeSeriesUpdaterInternalMethods<?, ?> parent,
            final long precedingMemoryOffset) {
        final File dataDirectory = parent.getLookupTable().getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        final String epochLocalDataId = parent.getEpochLocalDataId();
        if (epochLocalDataId != null) {
            return newEpochLocalMemoryFile(dataDirectory, epochLocalDataId, precedingMemoryOffset);
        }
        return newMemoryFile(new File(dataDirectory, MEMORY_DATA_FILE_NAME), precedingMemoryOffset);
    }

    /**
     * Data files of an epoch local attempt are named after that attempt and are written once and never modified
     * afterwards: a redo cannot reach bytes that the currently published index still references (it gets files of its
     * own), and an abandoned attempt leaves nothing but garbage behind. Further chunks of the same attempt are suffixed
     * with the bytes their preceding chunks wrote, mirroring the offset suffix scheme that the shared layout already
     * uses on Windows, so the progress writers need no extra chunk counter.
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
                    "Epoch local data file already exists, refusing to write into bytes another update attempt may have published: "
                            + memoryFile);
        }
        return memoryFile;
    }

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