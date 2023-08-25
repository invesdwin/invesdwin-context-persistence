package de.invesdwin.context.persistence.timeseriesdb;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.system.array.IPrimitiveArrayAllocator;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.time.duration.Duration;

@Immutable
public final class TimeseriesProperties {

    public static final boolean FILE_BUFFER_CACHE_SEGMENTS_ENABLED;
    public static final boolean FILE_BUFFER_CACHE_PRELOAD_ENABLED;
    public static final boolean FILE_BUFFER_CACHE_MMAP_ENABLED;
    public static final int FILE_BUFFER_CACHE_MAX_SEGMENTS_COUNT;
    public static final int FILE_BUFFER_CACHE_MAX_MMAP_COUNT;
    public static final Duration FILE_BUFFER_CACHE_EVICTION_TIMEOUT;
    public static final IPrimitiveArrayAllocator FILE_BUFFER_CACHE_FLYWEIGHT_ARRAY_ALLOCATOR;

    static {
        final SystemProperties systemProperties = new SystemProperties(TimeseriesProperties.class);
        FILE_BUFFER_CACHE_SEGMENTS_ENABLED = systemProperties.getBoolean("FILE_BUFFER_CACHE_SEGMENTS_ENABLED");
        FILE_BUFFER_CACHE_PRELOAD_ENABLED = systemProperties.getBoolean("FILE_BUFFER_CACHE_PRELOAD_ENABLED");
        FILE_BUFFER_CACHE_MMAP_ENABLED = systemProperties.getBoolean("FILE_BUFFER_CACHE_MMAP_ENABLED");
        FILE_BUFFER_CACHE_MAX_SEGMENTS_COUNT = systemProperties.getInteger("FILE_BUFFER_CACHE_MAX_SEGMENTS_COUNT");
        FILE_BUFFER_CACHE_MAX_MMAP_COUNT = systemProperties.getInteger("FILE_BUFFER_CACHE_MAX_MMAP_COUNT");
        FILE_BUFFER_CACHE_EVICTION_TIMEOUT = systemProperties.getDuration("FILE_BUFFER_CACHE_EVICTION_TIMEOUT");
        FILE_BUFFER_CACHE_FLYWEIGHT_ARRAY_ALLOCATOR = null;
    }

    private TimeseriesProperties() {}

}
