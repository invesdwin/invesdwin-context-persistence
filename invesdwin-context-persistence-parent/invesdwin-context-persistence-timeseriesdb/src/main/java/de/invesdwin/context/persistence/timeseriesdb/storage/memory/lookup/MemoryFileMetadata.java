package de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.filechannel.info.path.FileChannelPath;
import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannel;
import de.invesdwin.context.integration.filechannel.nio.atomic.properties.TransactionalFileProperties;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.system.properties.ICloseableProperties;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLockRegistry;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class MemoryFileMetadata {

    public static final long MISSING_EXPECTED_MEMORY_FILE_SIZE = -1L;
    private static final String KEY_EXPECTED_MEMORY_FILE_SIZE = "EXPECTED_MEMORY_FILE_SIZE";
    private final File dataDirectory;
    private final File logFile;
    private AtomicNioFileChannel propertiesPath;

    public MemoryFileMetadata(final File dataDirectory) {
        this.dataDirectory = dataDirectory;
        this.logFile = new File(dataDirectory, "memory.log");
    }

    public ICloseableProperties getProperties() {
        return new TransactionalFileProperties(this::getPropertiesPath);
    }

    private AtomicNioFileChannel getPropertiesPath() {
        if (propertiesPath == null) {
            synchronized (this) {
                if (propertiesPath == null) {
                    propertiesPath = new AtomicNioFileChannel(FileChannelPath
                            .newFile(new File(new File(dataDirectory, "properties"), "memory.properties")));
                }
            }
        }
        return propertiesPath;
    }

    public void setExpectedMemoryFileSize(final IProperties properties, final long memoryFileSize) {
        properties.setLong(KEY_EXPECTED_MEMORY_FILE_SIZE, memoryFileSize);
    }

    public long getExpectedMemoryFileSize(final IProperties properties) {
        return properties.getLongOptional(KEY_EXPECTED_MEMORY_FILE_SIZE, MISSING_EXPECTED_MEMORY_FILE_SIZE);
    }



    /**
     * Exactly ONE record per update attempt, written when the attempt's transaction closes - whether it committed or
     * aborted - stating what the store holds up to that point: the attempt id, how many data files it staged, how many
     * times it published, and the published frontier.
     *
     * <p>
     * The trail is kept because it is read by people, not by code: it is what dated "the last write of real tick data"
     * during the 2026-11-11 iqfeed outage (INCIDENT-iqfeed-outage-20261111.md:86). It used to be written once per
     * SUMMARY: measured 2026-11-14 09:2x from lap-mid over ssh on ptstaging's live store, 6,535 memory.log files
     * totalling 3,139.7 MB, the largest 42.2 MB holding 73,139 records for a key whose directory carries 22 index
     * generations. Per attempt is the unit that answers the operator question, and an attempt that staged nothing
     * writes nothing at all.
     */
    public void logTransaction(final String epochLocalDataId, final boolean committed, final int dataFilesStaged,
            final int publishes, final MemoryFileSummary lastPublished, final FDate lastPublishedValueEndTime,
            final long chainHighWaterBytes) {
        final StringBuilder logEntry = new StringBuilder();
        logEntry.append("\nTRANSACTION\n");
        logEntry.append("REAL_TIME=");
        logEntry.append(FDate.now());
        logEntry.append("\nHEARTBEAT_OWNER=");
        logEntry.append(HeartbeatFileChannelLockRegistry.HEARTBEAT_OWNER);
        logEntry.append("\nEPOCH_LOCAL_DATA_ID=");
        logEntry.append(epochLocalDataId);
        logEntry.append("\nOUTCOME=");
        logEntry.append(committed ? "COMMITTED" : "ABORTED");
        logEntry.append("\nDATA_FILES_STAGED=");
        logEntry.append(dataFilesStaged);
        logEntry.append("\nPUBLISHES=");
        logEntry.append(publishes);
        logEntry.append("\nCHAIN_HIGH_WATER_BYTES=");
        logEntry.append(chainHighWaterBytes);
        logEntry.append("\nLAST_PUBLISHED_VALUE_END_TIME=");
        logEntry.append(lastPublishedValueEndTime);
        if (lastPublished != null) {
            logEntry.append("\nLAST_PUBLISHED_FIRST_VALUE_END_TIME=");
            logEntry.append(lastPublished.getFirstValueEndTime());
            logEntry.append("\nMEMORY_RESOURCE_URI=");
            logEntry.append(lastPublished.getMemoryResourceUri());
            logEntry.append("\nMEMORY_OFFSET=");
            logEntry.append(lastPublished.getMemoryOffset());
            logEntry.append("\nMEMORY_LENGTH=");
            logEntry.append(lastPublished.getMemoryLength());
            logEntry.append("\nPRECEDING_VALUE_COUNT=");
            logEntry.append(lastPublished.getPrecedingValueCount());
            logEntry.append("\nVALUE_COUNT=");
            logEntry.append(lastPublished.getValueCount());
        }
        logEntry.append("\n");
        try {
            try (FileOutputStream out = new FileOutputStream(logFile, true)) {
                out.write(logEntry.toString().getBytes());
            }
        } catch (final FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

}
