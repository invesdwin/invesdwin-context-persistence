package de.invesdwin.context.persistence.timeseriesdb;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.system.array.primitive.IPrimitiveArrayAllocator;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.time.duration.Duration;

@Immutable
public final class TimeSeriesProperties {

    public static final Duration ACQUIRE_WRITE_LOCK_TIMEOUT;
    public static final Duration ACQUIRE_UPDATE_LOCK_TIMEOUT;
    public static final Duration UPDATE_ATTEMPT_ACQUIRE_WRITE_LOCK_TIMEOUT;
    public static final boolean FILE_BUFFER_CACHE_SEGMENTS_ENABLED;
    public static final boolean FILE_BUFFER_CACHE_PRELOAD_ENABLED;
    public static final boolean FILE_BUFFER_CACHE_MMAP_ENABLED;
    public static final int FILE_BUFFER_CACHE_MIN_SEGMENTS_COUNT;
    public static final int FILE_BUFFER_CACHE_MAX_SEGMENTS_COUNT;
    public static final int FILE_BUFFER_CACHE_MAX_MMAP_COUNT;
    public static final Duration FILE_BUFFER_CACHE_EVICTION_TIMEOUT;
    public static final Duration FILE_BUFFER_CACHE_ASYNC_TIMEOUT;
    public static final IPrimitiveArrayAllocator FILE_BUFFER_CACHE_FLYWEIGHT_ARRAY_ALLOCATOR;
    public static final Duration STORAGE_CACHE_EVICTION_TIMEOUT;
    public static final int STORAGE_CACHE_MAXIMUM_SIZE;
    public static final boolean PERSISTENT_CHRONICLE_MAP_ENABLED;
    public static final Duration RETAIN_OBSOLETE_FILES_THRESHOLD;
    public static final Duration RETAIN_OBSOLETE_VERSIONS_THRESHOLD;
    public static final Duration RETAIN_OBSOLETE_EPOCHS_THRESHOLD;
    public static final Duration UPDATE_YIELD_ACQUIRE_WRITE_LOCK_TIMEOUT;
    /**
     * Ceiling for the join onto an update that another context is already running, and it applies ONLY to a
     * non-blocking caller ({@code UpdateLockAcquirePolicy.isRequestTier()}): that caller answers "later" and must not
     * park, so past this ceiling it gets the PENDING verdict its own retry re-asks. A caller that may block waits on
     * the update lock without a ceiling instead - the join on the task behind it has always been unbounded by design,
     * so a ceiling on the lock alone only converts a slow-but-progressing update into a verdict the caller cannot use.
     * <p>
     * Sized as a watchdog for the non-blocking tier, not as a tuning knob. The claim that no normal round comes close
     * to it was checked against a cold umbrella run on 2026-11-22 and the same magnitude on the precalculated updater
     * creation path fired 33 times there, which is why that path no longer has a ceiling for blocking callers.
     */
    public static final Duration UPDATE_JOIN_ACQUIRE_TIMEOUT;
    private static final SystemProperties SYSTEM_PROPERTIES;

    static {
        SYSTEM_PROPERTIES = new SystemProperties(TimeSeriesProperties.class);
        ACQUIRE_WRITE_LOCK_TIMEOUT = SYSTEM_PROPERTIES.getDuration("ACQUIRE_WRITE_LOCK_TIMEOUT");
        ACQUIRE_UPDATE_LOCK_TIMEOUT = SYSTEM_PROPERTIES.getDuration("ACQUIRE_UPDATE_LOCK_TIMEOUT");
        UPDATE_ATTEMPT_ACQUIRE_WRITE_LOCK_TIMEOUT = SYSTEM_PROPERTIES
                .getDuration("UPDATE_ATTEMPT_ACQUIRE_WRITE_LOCK_TIMEOUT");
        FILE_BUFFER_CACHE_SEGMENTS_ENABLED = SYSTEM_PROPERTIES.getBoolean("FILE_BUFFER_CACHE_SEGMENTS_ENABLED");
        FILE_BUFFER_CACHE_PRELOAD_ENABLED = SYSTEM_PROPERTIES.getBoolean("FILE_BUFFER_CACHE_PRELOAD_ENABLED");
        FILE_BUFFER_CACHE_MMAP_ENABLED = SYSTEM_PROPERTIES.getBoolean("FILE_BUFFER_CACHE_MMAP_ENABLED");
        FILE_BUFFER_CACHE_MIN_SEGMENTS_COUNT = SYSTEM_PROPERTIES.getInteger("FILE_BUFFER_CACHE_MIN_SEGMENTS_COUNT");
        FILE_BUFFER_CACHE_MAX_SEGMENTS_COUNT = SYSTEM_PROPERTIES.getInteger("FILE_BUFFER_CACHE_MAX_SEGMENTS_COUNT");
        FILE_BUFFER_CACHE_MAX_MMAP_COUNT = SYSTEM_PROPERTIES.getInteger("FILE_BUFFER_CACHE_MAX_MMAP_COUNT");
        FILE_BUFFER_CACHE_EVICTION_TIMEOUT = SYSTEM_PROPERTIES.getDuration("FILE_BUFFER_CACHE_EVICTION_TIMEOUT");
        FILE_BUFFER_CACHE_ASYNC_TIMEOUT = SYSTEM_PROPERTIES.getDuration("FILE_BUFFER_CACHE_ASYNC_TIMEOUT");
        STORAGE_CACHE_EVICTION_TIMEOUT = SYSTEM_PROPERTIES.getDuration("STORAGE_CACHE_EVICTION_TIMEOUT");
        STORAGE_CACHE_MAXIMUM_SIZE = SYSTEM_PROPERTIES.getInteger("STORAGE_CACHE_MAXIMUM_SIZE");
        PERSISTENT_CHRONICLE_MAP_ENABLED = determinePersistentChronicleMapEnabled();
        RETAIN_OBSOLETE_FILES_THRESHOLD = SYSTEM_PROPERTIES.getDuration("RETAIN_OBSOLETE_FILES_THRESHOLD");
        RETAIN_OBSOLETE_VERSIONS_THRESHOLD = SYSTEM_PROPERTIES.getDuration("RETAIN_OBSOLETE_VERSIONS_THRESHOLD");
        RETAIN_OBSOLETE_EPOCHS_THRESHOLD = SYSTEM_PROPERTIES.getDuration("RETAIN_OBSOLETE_EPOCHS_THRESHOLD");
        UPDATE_YIELD_ACQUIRE_WRITE_LOCK_TIMEOUT = SYSTEM_PROPERTIES
                .getDuration("UPDATE_YIELD_ACQUIRE_WRITE_LOCK_TIMEOUT");
        UPDATE_JOIN_ACQUIRE_TIMEOUT = SYSTEM_PROPERTIES.getDuration("UPDATE_JOIN_ACQUIRE_TIMEOUT");
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

    public static Duration newAcquireFileLockTimeout() {
        return ACQUIRE_WRITE_LOCK_TIMEOUT.newRandomDuration();
    }

}
