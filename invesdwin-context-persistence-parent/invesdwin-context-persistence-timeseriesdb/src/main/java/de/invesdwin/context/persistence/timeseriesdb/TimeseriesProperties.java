package de.invesdwin.context.persistence.timeseriesdb;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.lang.OperatingSystem;
import de.invesdwin.util.time.duration.Duration;

@Immutable
public final class TimeseriesProperties {

    public static final boolean FILE_BUFFER_CACHE_SEGMENTS_ENABLED;
    public static final boolean FILE_BUFFER_CACHE_PRELOAD_ENABLED;
    public static final boolean FILE_BUFFER_CACHE_MMAP_ENABLED;
    public static final int FILE_BUFFER_CACHE_MAX_SEGMENTS_COUNT;
    public static final int FILE_BUFFER_CACHE_MAX_MMAP_COUNT;
    public static final Duration FILE_BUFFER_CACHE_EVICTION_TIMEOUT;

    static {
        final SystemProperties systemProperties = new SystemProperties(TimeseriesProperties.class);
        FILE_BUFFER_CACHE_SEGMENTS_ENABLED = systemProperties.getBoolean("FILE_BUFFER_CACHE_SEGMENTS_ENABLED");
        FILE_BUFFER_CACHE_PRELOAD_ENABLED = systemProperties.getBoolean("FILE_BUFFER_CACHE_PRELOAD_ENABLED");
        FILE_BUFFER_CACHE_MMAP_ENABLED = readFileBufferCacheMmapEnabled(systemProperties);
        FILE_BUFFER_CACHE_MAX_SEGMENTS_COUNT = systemProperties.getInteger("FILE_BUFFER_CACHE_MAX_SEGMENTS_COUNT");
        FILE_BUFFER_CACHE_MAX_MMAP_COUNT = systemProperties.getInteger("FILE_BUFFER_CACHE_MAX_MMAP_COUNT");
        FILE_BUFFER_CACHE_EVICTION_TIMEOUT = systemProperties.getDuration("FILE_BUFFER_CACHE_EVICTION_TIMEOUT");
    }

    private TimeseriesProperties() {}

    /**
     * TicksProcessingSpeedTest on JFOREX:EURUSD fails when iterating through the whole history on windows. Maybe
     * windows has some size limit on memory mapped files.
     * 
     * We are not the only ones that don't support memory mapped files on windows:
     * https://mapdb.org/blog/mmap_files_alloc_and_jvm_crash/
     */
    private static Boolean readFileBufferCacheMmapEnabled(final SystemProperties systemProperties) {
        if (OperatingSystem.isWindows()) {
            return false;
        } else {
            return systemProperties.getBoolean("FILE_BUFFER_CACHE_MMAP_ENABLED");
        }
    }
}
