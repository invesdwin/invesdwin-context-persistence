package de.invesdwin.context.persistence.timeseriesdb;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.time.duration.Duration;

@Immutable
public final class TimeseriesProperties {

    public static final boolean FILE_BUFFER_CACHE_ENABLED;
    public static final int FILE_BUFFER_CACHE_MAX_COUNT;
    public static final Duration FILE_BUFFER_CACHE_EVICTION_TIMEOUT;

    static {
        final SystemProperties systemProperties = new SystemProperties(TimeseriesProperties.class);
        FILE_BUFFER_CACHE_ENABLED = systemProperties.getBoolean("FILE_BUFFER_CACHE_ENABLED");
        FILE_BUFFER_CACHE_MAX_COUNT = systemProperties.getInteger("FILE_BUFFER_CACHE_MAX_COUNT");
        FILE_BUFFER_CACHE_EVICTION_TIMEOUT = systemProperties.getDuration("FILE_BUFFER_CACHE_EVICTION_TIMEOUT");
    }

    private TimeseriesProperties() {
    }

}