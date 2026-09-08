package de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.filechannel.info.path.FileChannelPath;
import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannel;
import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannelPath;
import de.invesdwin.context.integration.filechannel.nio.atomic.properties.TransactionalFileProperties;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.system.properties.ICloseableProperties;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.concurrent.lock.FileChannelLockHeartbeatRegistry;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class MemoryFileMetadata {

    public static final long MISSING_EXPECTED_MEMORY_FILE_SIZE = -1L;
    private static final String KEY_EXPECTED_MEMORY_FILE_SIZE = "EXPECTED_MEMORY_FILE_SIZE";
    private final File dataDirectory;
    private final File logFile;
    private AtomicNioFileChannelPath propertiesPath;

    public MemoryFileMetadata(final File dataDirectory) {
        this.dataDirectory = dataDirectory;
        this.logFile = new File(dataDirectory, "memory.log");
    }

    public ICloseableProperties getProperties() {
        return new TransactionalFileProperties(getPropertiesPath());
    }

    private AtomicNioFileChannelPath getPropertiesPath() {
        if (propertiesPath == null) {
            synchronized (this) {
                if (propertiesPath == null) {
                    propertiesPath = new AtomicNioFileChannelPath(FileChannelPath.valueOfFile(
                            new File(new File(dataDirectory, "properties"), "memory.properties").toURI(),
                            AtomicNioFileChannel.DEFAULT_SERVER_URI_F));
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

    public void setSummary(final FDate fistValueEndTime, final FDate lastValueEndTime, final long precedingValueCount,
            final int valueCount, final String memoryResourceUri, final long precedingMemoryOffset,
            final long memoryOffset, final long memoryLength) {
        final StringBuilder logEntry = new StringBuilder();
        logEntry.append("\n");
        logEntry.append(MemoryFileSummary.class.getSimpleName());
        logEntry.append("\nREAL_TIME=");
        logEntry.append(FDate.now());
        logEntry.append("\nHEARTBEAT_OWNER=");
        logEntry.append(FileChannelLockHeartbeatRegistry.HEARTBEAT_OWNER);
        logEntry.append("\nFIRST_VALUE_END_TIME=");
        logEntry.append(fistValueEndTime);
        logEntry.append("\nLAST_VALUE_END_TIME=");
        logEntry.append(lastValueEndTime);
        logEntry.append("\nMEMORY_RESOURCE_URI=");
        logEntry.append(memoryResourceUri);
        logEntry.append("\nPRECEDING_MEMORY_OFFSET=");
        logEntry.append(precedingMemoryOffset);
        logEntry.append("\nMEMORY_OFFSET=");
        logEntry.append(memoryOffset);
        logEntry.append("\nMEMORY_LENGTH=");
        logEntry.append(memoryLength);
        logEntry.append("\nPRECEDING_VALUE_COUNT=");
        logEntry.append(precedingValueCount);
        logEntry.append("\nVALUE_COUNT=");
        logEntry.append(valueCount);
        logEntry.append("\n");
        try (FileOutputStream out = new FileOutputStream(logFile, true)) {
            out.write(logEntry.toString().getBytes());
        } catch (final FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

}
