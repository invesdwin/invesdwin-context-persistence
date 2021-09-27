package de.invesdwin.context.persistence.timeseriesdb;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;
import de.invesdwin.context.persistence.chronicle.PersistentChronicleMapFactory;
import de.invesdwin.context.persistence.mapdb.PersistentMapDBFactory;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.time.duration.Duration;

@Immutable
public final class TimeseriesProperties {

    public static final boolean FILE_BUFFER_CACHE_ENABLED;
    public static final boolean FILE_BUFFER_CACHE_PRELOAD_ENABLED;
    public static final int FILE_BUFFER_CACHE_MAX_SEGMENTS_COUNT;
    public static final int FILE_BUFFER_CACHE_MAX_OPEN_FILES_COUNT;
    public static final Duration FILE_BUFFER_CACHE_EVICTION_TIMEOUT;

    static {
        final SystemProperties systemProperties = new SystemProperties(TimeseriesProperties.class);
        FILE_BUFFER_CACHE_ENABLED = systemProperties.getBoolean("FILE_BUFFER_CACHE_ENABLED");
        FILE_BUFFER_CACHE_PRELOAD_ENABLED = systemProperties.getBoolean("FILE_BUFFER_CACHE_PRELOAD_ENABLED");
        FILE_BUFFER_CACHE_MAX_SEGMENTS_COUNT = systemProperties.getInteger("FILE_BUFFER_CACHE_MAX_SEGMENTS_COUNT");
        FILE_BUFFER_CACHE_MAX_OPEN_FILES_COUNT = systemProperties.getInteger("FILE_BUFFER_CACHE_MAX_OPEN_FILES_COUNT");
        FILE_BUFFER_CACHE_EVICTION_TIMEOUT = systemProperties.getDuration("FILE_BUFFER_CACHE_EVICTION_TIMEOUT");
    }

    private TimeseriesProperties() {
    }

    public static <K, V> IPersistentMapFactory<K, V> newPersistentMapFactory(final boolean large) {
        if (large) {
            //mapdb has a smaller footprint on disk, thus prefer that for larger entries
            return new PersistentMapDBFactory<K, V>();
        } else {
            //otherwise use chronicle map for faster put/get
            return new PersistentChronicleMapFactory<>();
        }
    }

}
