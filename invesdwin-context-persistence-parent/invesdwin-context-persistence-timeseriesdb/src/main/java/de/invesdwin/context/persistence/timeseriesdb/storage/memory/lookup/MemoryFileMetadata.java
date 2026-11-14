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
     * One record per published generation, appended to the key's {@code memory.log}: what the publish left at the head
     * of the chain, and how many summaries it added.
     *
     * <p>
     * Kept deliberately rather than deleted: this file is what dated "the last write of real tick data" during the
     * 2026-11-11 iqfeed outage (its mtime plus the record's own timestamps), and nothing in the code root reads it.
     *
     * <p>
     * It used to be written once PER SUMMARY, which on the live store is pure volume without information: measured
     * 2026-11-14 from lap-mid over ssh on ptstaging's ~/.invesdwin, 6,535 memory.log files totalling 3,139 MB, the
     * largest 42 MB holding 73,139 records for a key whose directory carries 22 index generations - because each
     * publish added its whole batch of summaries again. One record per publish keeps the same fields and the same
     * frontier, at a tenth of the text for interval 10. The per-summary write was also 6.0 us of open+append+close
     * each (measured on this box, ext4) against 0.5 us buffered, though at 10 summaries per publish that is only
     * ~0.7 % of the ~8.3 ms a publish costs overall - the disk and the noise are the reason, not the CPU.
     */
    public void logPublish(final int publishedSummaryCount, final MemoryFileSummary lastPublished,
            final long chainHighWaterBytes, final FDate lastValueEndTime) {
        final StringBuilder logEntry = new StringBuilder();
        logEntry.append("\nPUBLISH\n");
        logEntry.append("REAL_TIME=");
        logEntry.append(FDate.now());
        logEntry.append("\nHEARTBEAT_OWNER=");
        logEntry.append(HeartbeatFileChannelLockRegistry.HEARTBEAT_OWNER);
        logEntry.append("\nPUBLISHED_SUMMARY_COUNT=");
        logEntry.append(publishedSummaryCount);
        logEntry.append("\nCHAIN_HIGH_WATER_BYTES=");
        logEntry.append(chainHighWaterBytes);
        logEntry.append("\nLAST_SUMMARY_FIRST_VALUE_END_TIME=");
        logEntry.append(lastPublished.getFirstValueEndTime());
        logEntry.append("\nLAST_SUMMARY_LAST_VALUE_END_TIME=");
        logEntry.append(lastValueEndTime);
        logEntry.append("\nMEMORY_RESOURCE_URI=");
        logEntry.append(lastPublished.getMemoryResourceUri());
        logEntry.append("\nPRECEDING_MEMORY_OFFSET=");
        logEntry.append(lastPublished.getPrecedingMemoryOffset());
        logEntry.append("\nMEMORY_OFFSET=");
        logEntry.append(lastPublished.getMemoryOffset());
        logEntry.append("\nMEMORY_LENGTH=");
        logEntry.append(lastPublished.getMemoryLength());
        logEntry.append("\nPRECEDING_VALUE_COUNT=");
        logEntry.append(lastPublished.getPrecedingValueCount());
        logEntry.append("\nVALUE_COUNT=");
        logEntry.append(lastPublished.getValueCount());
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
