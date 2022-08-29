package de.invesdwin.context.persistence.timeseriesdb.storage;

import java.io.File;

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

    public MemoryFileMetadata(final File file) {
        this.properties = new CachingDelegateProperties(new FileProperties(file));
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
            final int valueCount, final String memoryResourceUri, final long memoryOffset, final long memoryLength) {
        final String key = MemoryFileSummary.class.getSimpleName() + "." + memoryOffset + ".";
        properties.setLong(key + "MEMORY_LENGTH", memoryLength);
        properties.setDate(key + "REAL_TIME", new FDate());
        properties.setDate(key + "INDEX_TIME", time);
        properties.setDate(key + "FIRST_VALUE_DATE", firstValueDate);
        properties.setDate(key + "LAST_VALUE_DATE", lastValueDate);
        properties.setInteger(key + "VALUE_COUNT", valueCount);
        properties.setString(key + "MEMORY_RESOURCE_URI", memoryResourceUri);
    }

}
