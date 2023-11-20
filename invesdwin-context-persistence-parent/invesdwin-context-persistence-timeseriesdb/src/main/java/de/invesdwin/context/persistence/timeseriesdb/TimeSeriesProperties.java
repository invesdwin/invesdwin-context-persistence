package de.invesdwin.context.persistence.timeseriesdb;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.system.array.IPrimitiveArrayAllocator;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;

@Immutable
public final class TimeSeriesProperties {

    public static final Duration ACQUIRE_WRITE_LOCK_TIMEOUT;
    public static final Duration ACQUIRE_UPDATE_LOCK_TIMEOUT;
    public static final boolean FILE_BUFFER_CACHE_SEGMENTS_ENABLED;
    public static final boolean FILE_BUFFER_CACHE_PRELOAD_ENABLED;
    public static final boolean FILE_BUFFER_CACHE_MMAP_ENABLED;
    public static final int FILE_BUFFER_CACHE_MAX_SEGMENTS_COUNT;
    public static final int FILE_BUFFER_CACHE_MAX_MMAP_COUNT;
    public static final Duration FILE_BUFFER_CACHE_EVICTION_TIMEOUT;
    public static final Duration FILE_BUFFER_CACHE_ASYNC_TIMEOUT;
    public static final IPrimitiveArrayAllocator FILE_BUFFER_CACHE_FLYWEIGHT_ARRAY_ALLOCATOR;
    private static final SystemProperties SYSTEM_PROPERTIES;

    static {
        SYSTEM_PROPERTIES = new SystemProperties(TimeSeriesProperties.class);
        ACQUIRE_WRITE_LOCK_TIMEOUT = SYSTEM_PROPERTIES.getDuration("ACQUIRE_WRITE_LOCK_TIMEOUT");
        ACQUIRE_UPDATE_LOCK_TIMEOUT = SYSTEM_PROPERTIES.getDuration("ACQUIRE_UPDATE_LOCK_TIMEOUT");
        FILE_BUFFER_CACHE_SEGMENTS_ENABLED = SYSTEM_PROPERTIES.getBoolean("FILE_BUFFER_CACHE_SEGMENTS_ENABLED");
        FILE_BUFFER_CACHE_PRELOAD_ENABLED = SYSTEM_PROPERTIES.getBoolean("FILE_BUFFER_CACHE_PRELOAD_ENABLED");
        FILE_BUFFER_CACHE_MMAP_ENABLED = SYSTEM_PROPERTIES.getBoolean("FILE_BUFFER_CACHE_MMAP_ENABLED");
        FILE_BUFFER_CACHE_MAX_SEGMENTS_COUNT = SYSTEM_PROPERTIES.getInteger("FILE_BUFFER_CACHE_MAX_SEGMENTS_COUNT");
        FILE_BUFFER_CACHE_MAX_MMAP_COUNT = SYSTEM_PROPERTIES.getInteger("FILE_BUFFER_CACHE_MAX_MMAP_COUNT");
        FILE_BUFFER_CACHE_EVICTION_TIMEOUT = SYSTEM_PROPERTIES.getDuration("FILE_BUFFER_CACHE_EVICTION_TIMEOUT");
        FILE_BUFFER_CACHE_ASYNC_TIMEOUT = SYSTEM_PROPERTIES.getDuration("FILE_BUFFER_CACHE_ASYNC_TIMEOUT");
        FILE_BUFFER_CACHE_FLYWEIGHT_ARRAY_ALLOCATOR = null;
    }

    private TimeSeriesProperties() {}

    public static FDate getUpdateLimitTo() {
        return SYSTEM_PROPERTIES.getDateOptional("UPDATE_LIMIT_TO");
    }

    public static void setUpdateLimitTo(final FDate updateLimitTo) {
        SYSTEM_PROPERTIES.setDate("UPDATE_LIMIT_TO", updateLimitTo);
    }

    public static void setUpdateLimitTo() {
        setUpdateLimitTo(new FDate());
    }

}
