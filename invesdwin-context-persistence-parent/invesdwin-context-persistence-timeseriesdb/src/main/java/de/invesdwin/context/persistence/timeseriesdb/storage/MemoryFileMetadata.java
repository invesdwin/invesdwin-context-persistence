package de.invesdwin.context.persistence.timeseriesdb.storage;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.system.properties.FileProperties;

@NotThreadSafe
public class MemoryFileMetadata {

    public static final long MISSING_MEMORY_FILE_SIZE = -1L;
    private static final String KEY_MEMORY_FILE_SIZE = "MEMORY_FILE_SIZE";
    private final FileProperties properties;

    public MemoryFileMetadata(final File file) {
        this.properties = new FileProperties(file);
    }

    public void setMemoryFileSize(final long memoryFileSize) {
        properties.setLong(KEY_MEMORY_FILE_SIZE, memoryFileSize);
    }

    public long getMemoryFileSize() {
        if (properties.containsKey(KEY_MEMORY_FILE_SIZE)) {
            return properties.getLong(KEY_MEMORY_FILE_SIZE);
        } else {
            return MISSING_MEMORY_FILE_SIZE;
        }
    }

}
