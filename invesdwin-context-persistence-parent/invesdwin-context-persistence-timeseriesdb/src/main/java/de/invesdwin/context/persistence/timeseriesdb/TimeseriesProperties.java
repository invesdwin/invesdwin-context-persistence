package de.invesdwin.context.persistence.timeseriesdb;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.time.duration.Duration;

@Immutable
public final class TimeseriesProperties {

    public static final int FILE_BUFFER_CACHE_MAX_ENTRIES;
    public static final Duration FILE_BUFFER_CACHE_ENTRY_TIMEOUT;

    static {
        final SystemProperties systemProperties = new SystemProperties(TimeseriesProperties.class);
        FILE_BUFFER_CACHE_MAX_ENTRIES = systemProperties.getInteger("FILE_BUFFER_CACHE_MAX_ENTRIES");
        FILE_BUFFER_CACHE_ENTRY_TIMEOUT = systemProperties.getDuration("FILE_BUFFER_CACHE_ENTRY_TIMEOUT");
    }

    private TimeseriesProperties() {
    }

}
