package de.invesdwin.context.persistence.timeseriesdb;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.system.array.IPrimitiveArrayAllocator;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.time.duration.Duration;

@Immutable
public final class TimeSeriesProperties {

    public static final Duration NON_BLOCKING_ASYNC_UPDATE_WAIT_TIMEOUT;
    public static final Duration ACQUIRE_WRITE_LOCK_TIMEOUT;
    public static final Duration ACQUIRE_UPDATE_LOCK_TIMEOUT;
    public static final boolean FILE_BUFFER_CACHE_SEGMENTS_ENABLED;
    public static final boolean FILE_BUFFER_CACHE_PRELOAD_ENABLED;
    public static final boolean FILE_BUFFER_CACHE_MMAP_ENABLED;
    public static final int FILE_BUFFER_CACHE_MIN_SEGMENTS_COUNT;
    public static final int FILE_BUFFER_CACHE_MAX_SEGMENTS_COUNT;
    public static final int FILE_BUFFER_CACHE_MAX_MMAP_COUNT;
    public static final Duration FILE_BUFFER_CACHE_EVICTION_TIMEOUT;
    public static final Duration FILE_BUFFER_CACHE_ASYNC_TIMEOUT;
    public static final IPrimitiveArrayAllocator FILE_BUFFER_CACHE_FLYWEIGHT_ARRAY_ALLOCATOR;
    public static final boolean PERSISTENT_CHRONICLE_MAP_ENABLED;
    private static final SystemProperties SYSTEM_PROPERTIES;

    static {
        SYSTEM_PROPERTIES = new SystemProperties(TimeSeriesProperties.class);
        NON_BLOCKING_ASYNC_UPDATE_WAIT_TIMEOUT = SYSTEM_PROPERTIES
                .getDuration("NON_BLOCKING_ASYNC_UPDATE_WAIT_TIMEOUT");
        ACQUIRE_WRITE_LOCK_TIMEOUT = SYSTEM_PROPERTIES.getDuration("ACQUIRE_WRITE_LOCK_TIMEOUT");
        ACQUIRE_UPDATE_LOCK_TIMEOUT = SYSTEM_PROPERTIES.getDuration("ACQUIRE_UPDATE_LOCK_TIMEOUT");
        FILE_BUFFER_CACHE_SEGMENTS_ENABLED = SYSTEM_PROPERTIES.getBoolean("FILE_BUFFER_CACHE_SEGMENTS_ENABLED");
        FILE_BUFFER_CACHE_PRELOAD_ENABLED = SYSTEM_PROPERTIES.getBoolean("FILE_BUFFER_CACHE_PRELOAD_ENABLED");
        FILE_BUFFER_CACHE_MMAP_ENABLED = SYSTEM_PROPERTIES.getBoolean("FILE_BUFFER_CACHE_MMAP_ENABLED");
        FILE_BUFFER_CACHE_MIN_SEGMENTS_COUNT = SYSTEM_PROPERTIES.getInteger("FILE_BUFFER_CACHE_MIN_SEGMENTS_COUNT");
        FILE_BUFFER_CACHE_MAX_SEGMENTS_COUNT = SYSTEM_PROPERTIES.getInteger("FILE_BUFFER_CACHE_MAX_SEGMENTS_COUNT");
        FILE_BUFFER_CACHE_MAX_MMAP_COUNT = SYSTEM_PROPERTIES.getInteger("FILE_BUFFER_CACHE_MAX_MMAP_COUNT");
        FILE_BUFFER_CACHE_EVICTION_TIMEOUT = SYSTEM_PROPERTIES.getDuration("FILE_BUFFER_CACHE_EVICTION_TIMEOUT");
        FILE_BUFFER_CACHE_ASYNC_TIMEOUT = SYSTEM_PROPERTIES.getDuration("FILE_BUFFER_CACHE_ASYNC_TIMEOUT");
        PERSISTENT_CHRONICLE_MAP_ENABLED = determinePersistentChronicleMapEnabled();
        FILE_BUFFER_CACHE_FLYWEIGHT_ARRAY_ALLOCATOR = null;
    }

    private TimeSeriesProperties() {}

    private static boolean determinePersistentChronicleMapEnabled() {
        final String key = "PERSISTENT_CHRONICLE_MAP_ENABLED";
        if (SYSTEM_PROPERTIES.containsValue(key)) {
            return SYSTEM_PROPERTIES.getBoolean(key);
        } else {
            final boolean supported = isPersistentChronicleMapSupported();
            SYSTEM_PROPERTIES.setBoolean(key, supported);
            return supported;
        }
    }

    /**
     * https://github.com/NationalSecurityAgency/ghidra/issues/4291
     */
    private static boolean isPersistentChronicleMapSupported() {
        final File dir = ContextProperties.getHomeDataDirectory();
        try {
            return Files.getFileStore(dir.toPath()) != null;
        } catch (final IOException e) {
            new Log(TimeSeriesProperties.class).error("Failed to get java " + FileStore.class.getSimpleName()
                    + " for path " + dir
                    + ". This might happen in chrooted environments. Disabling chronicle map because it will be unable to check free/available space before memory mapping a file.");
            return false;
        }
    }

}
