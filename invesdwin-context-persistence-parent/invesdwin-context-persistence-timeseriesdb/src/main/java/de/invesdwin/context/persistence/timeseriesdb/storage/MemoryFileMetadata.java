package de.invesdwin.context.persistence.timeseriesdb.storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.system.properties.CachingDelegateProperties;
import de.invesdwin.context.system.properties.FileProperties;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class MemoryFileMetadata {

    public static final long MISSING_EXPECTED_MEMORY_FILE_SIZE = -1L;
    private static final String KEY_EXPECTED_MEMORY_FILE_SIZE = "EXPECTED_MEMORY_FILE_SIZE";
    private final IProperties properties;
    private final File logFile;

    public MemoryFileMetadata(final File dataDirectory) {
        this.properties = new CachingDelegateProperties(
                new FileProperties(new File(dataDirectory, "memory.properties")));
        this.logFile = new File(dataDirectory, "memory.log");
    }

    public void setExpectedMemoryFileSize(final long memoryFileSize) {
        properties.setLong(KEY_EXPECTED_MEMORY_FILE_SIZE, memoryFileSize);
    }

    public long getExpectedMemoryFileSize() {
        if (properties.containsKey(KEY_EXPECTED_MEMORY_FILE_SIZE)) {
            return properties.getLong(KEY_EXPECTED_MEMORY_FILE_SIZE);
        } else {
            return MISSING_EXPECTED_MEMORY_FILE_SIZE;
        }
    }

    public void setSummary(final FDate time, final FDate firstValueDate, final FDate lastValueDate,
            final long precedingValueCount, final int valueCount, final String memoryResourceUri,
            final long precedingMemoryOffset, final long memoryOffset, final long memoryLength) {
        final StringBuilder logEntry = new StringBuilder();
        logEntry.append("\n");
        logEntry.append(MemoryFileSummary.class.getSimpleName());
        logEntry.append("\nREAL_TIME=");
        logEntry.append(new FDate());
        logEntry.append("\nMEMORY_RESOURCE_URI=");
        logEntry.append(memoryResourceUri);
        logEntry.append("\nPRECEDING_MEMORY_OFFSET=");
        logEntry.append(precedingMemoryOffset);
        logEntry.append("\nMEMORY_OFFSET=");
        logEntry.append(memoryOffset);
        logEntry.append("\nMEMORY_LENGTH=");
        logEntry.append(memoryLength);
        logEntry.append("\nINDEX_TIME=");
        logEntry.append(time);
        logEntry.append("\nFIRST_VALUE_DATE=");
        logEntry.append(firstValueDate);
        logEntry.append("\nLAST_VALUE_DATE=");
        logEntry.append(lastValueDate);
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
