package de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.lease;

import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.ITimeSeriesDirectoryHashKey;
import de.invesdwin.instrument.DynamicInstrumentationProperties;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.fast.IFastIterableMap;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLock;
import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLockRegistry;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.millis.FDateMillis;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public final class TimeSeriesDirectoryHashKeyVersionLeaseRegistry {

    private static final Map<File, SharedDirectoryLeaseContext> DIRECTORY_CONTEXTS = ILockCollectionFactory
            .getInstance(true)
            .newConcurrentMap();
    private static final Duration HEARTBEAT_INTERVAL = Duration.ONE_MINUTE;
    private static final Duration CLEANUP_CHECK_INTERVAL = Duration.ONE_HOUR;
    private static final Duration CLEANUP_INTERVAL = Duration.ONE_DAY;

    private static final String CLEANUP_MARKER_FILENAME = ".cleanup";

    private static ScheduledExecutorService heartbeatExecutor;
    private static ScheduledExecutorService cleanupExecutor;

    private TimeSeriesDirectoryHashKeyVersionLeaseRegistry() {}

    public static TimeSeriesDirectoryHashKeyVersionLease getOrCreate(final ITimeSeriesDirectoryHashKey parent,
            final int version) {

        final File heartbeatDirectory = parent.getParent().getHeartbeatDirectory();
        final SharedDirectoryLeaseContext context = DIRECTORY_CONTEXTS.computeIfAbsent(heartbeatDirectory,
                SharedDirectoryLeaseContext::new);

        return context.getOrCreateLease(parent, version);
    }

    static void remove(final TimeSeriesDirectoryHashKeyVersionLease lease) {
        final File heartbeatDirectory = lease.getParent().getParent().getHeartbeatDirectory();
        DIRECTORY_CONTEXTS.computeIfPresent(heartbeatDirectory, (dir, context) -> {
            if (context.removeLeaseAndCheckEmpty(lease)) {
                return null;
            }
            return context;
        });

        stopHeartbeatExecutorIfNeeded();
        stopCleanupExecutorIfNeeded();
    }

    private static void startHeartbeatExecutorIfNeeded() {
        synchronized (TimeSeriesDirectoryHashKeyVersionLeaseRegistry.class) {
            if (heartbeatExecutor == null || heartbeatExecutor.isShutdown()) {
                heartbeatExecutor = Executors.newScheduledThreadPool(
                        TimeSeriesDirectoryHashKeyVersionLeaseRegistry.class.getSimpleName() + "-Heartbeat", 1);

                // Periodic background refresh for all active contexts
                heartbeatExecutor.scheduleAtFixedRate(() -> {
                    synchronized (DIRECTORY_CONTEXTS) {
                        updateHeartbeats();
                    }
                }, HEARTBEAT_INTERVAL.millisValue(), HEARTBEAT_INTERVAL.millisValue(), TimeUnit.MILLISECONDS);
            }
        }
    }

    private static void stopHeartbeatExecutorIfNeeded() {
        synchronized (TimeSeriesDirectoryHashKeyVersionLeaseRegistry.class) {
            if (DIRECTORY_CONTEXTS.isEmpty() && heartbeatExecutor != null && !heartbeatExecutor.isShutdown()) {
                heartbeatExecutor.shutdown();
                heartbeatExecutor = null;
            }
        }
    }

    private static void startCleanupExecutorIfNeeded() {
        synchronized (TimeSeriesDirectoryHashKeyVersionLeaseRegistry.class) {
            if (cleanupExecutor == null || cleanupExecutor.isShutdown()) {
                cleanupExecutor = Executors.newScheduledThreadPool(
                        TimeSeriesDirectoryHashKeyVersionLeaseRegistry.class.getSimpleName() + "-Cleanup", 1);

                // Check cleanup status hourly so restarts can pick up pending cleanups promptly
                cleanupExecutor.scheduleAtFixedRate(() -> {
                    final SharedDirectoryLeaseContext[] contexts;
                    synchronized (DIRECTORY_CONTEXTS) {
                        contexts = DIRECTORY_CONTEXTS.values().toArray(new SharedDirectoryLeaseContext[0]);
                    }
                    for (final SharedDirectoryLeaseContext context : contexts) {
                        context.cleanupObsoleteVersions();
                    }
                }, 0, CLEANUP_CHECK_INTERVAL.millisValue(), TimeUnit.MILLISECONDS);
            }
        }
    }

    private static void stopCleanupExecutorIfNeeded() {
        synchronized (TimeSeriesDirectoryHashKeyVersionLeaseRegistry.class) {
            if (DIRECTORY_CONTEXTS.isEmpty() && cleanupExecutor != null && !cleanupExecutor.isShutdown()) {
                cleanupExecutor.shutdown();
                cleanupExecutor = null;
            }
        }
    }

    private static void updateHeartbeats() {
        for (final SharedDirectoryLeaseContext context : DIRECTORY_CONTEXTS.values()) {
            if (!context.touchOrRewriteHeartbeat()) {
                DIRECTORY_CONTEXTS.computeIfPresent(context.getHeartbeatDirectory(),
                        (dir, ctx) -> ctx.isEmpty() ? null : ctx);
            }
        }
        stopHeartbeatExecutorIfNeeded();
        stopCleanupExecutorIfNeeded();
    }

    private static final class SharedDirectoryLeaseContext {
        private final File heartbeatDirectory;
        private final File heartbeatFile;
        private final FDate createdTimestamp = FDate.now();
        private final AtomicBoolean writeScheduled = new AtomicBoolean(false);

        @GuardedBy("activeLeases")
        private final IFastIterableMap<String, WeakTimeSeriesDirectoryHashKeyVersionLease> activeLeases = ILockCollectionFactory
                .getInstance(false)
                .newFastIterableMap();
        private WeakTimeSeriesDirectoryHashKeyVersionLease[] lastActiveLeasesSnapshot = WeakTimeSeriesDirectoryHashKeyVersionLease.EMPTY_ARRAY;

        private SharedDirectoryLeaseContext(final File heartbeatDirectory) {
            this.heartbeatDirectory = heartbeatDirectory;
            this.heartbeatFile = new File(heartbeatDirectory,
                    Files.normalizeFilename(HeartbeatFileChannelLockRegistry.HEARTBEAT_OWNER
                            + HeartbeatFileChannelLockRegistry.HEARTBEAT_EXTENSION));
            try {
                Files.forceMkdirParent(heartbeatFile);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

        public File getHeartbeatDirectory() {
            return heartbeatDirectory;
        }

        public TimeSeriesDirectoryHashKeyVersionLease getOrCreateLease(final ITimeSeriesDirectoryHashKey parent,
                final int version) {
            final String registryKey = newRegistryKey(parent, version);
            final TimeSeriesDirectoryHashKeyVersionLease newLease;

            synchronized (activeLeases) {
                final WeakTimeSeriesDirectoryHashKeyVersionLease existingRef = activeLeases.get(registryKey);
                if (existingRef != null) {
                    final TimeSeriesDirectoryHashKeyVersionLease existingLease = existingRef.get();
                    if (existingLease != null) {
                        existingLease.retain();
                        return existingLease;
                    }
                }
                newLease = new TimeSeriesDirectoryHashKeyVersionLease(registryKey, parent, version);
                newLease.retain();
                activeLeases.put(registryKey, new WeakTimeSeriesDirectoryHashKeyVersionLease(newLease));
            }

            requestWrite();
            return newLease;
        }

        private String newRegistryKey(final ITimeSeriesDirectoryHashKey parent, final int version) {
            return parent.getHashKey() + "/" + version;
        }

        public boolean removeLeaseAndCheckEmpty(final TimeSeriesDirectoryHashKeyVersionLease lease) {
            final boolean isEmpty;
            synchronized (activeLeases) {
                activeLeases.remove(lease.getRegistryKey());
                isEmpty = activeLeases.isEmpty();
            }

            if (isEmpty) {
                synchronized (DIRECTORY_CONTEXTS) {
                    deleteHeartbeat();
                }
                return true;
            } else {
                requestWrite();
                return false;
            }
        }

        public boolean isEmpty() {
            synchronized (activeLeases) {
                return activeLeases.isEmpty();
            }
        }

        public void cleanupObsoleteVersions() {
            final File cleanupMarkerFile = new File(heartbeatDirectory, CLEANUP_MARKER_FILENAME);
            final long now = FDateMillis.nowMillis();

            // Fast exit: Skip lock acquisition if the cleanup was executed within the last 24 hours
            if (cleanupMarkerFile.exists()
                    && CLEANUP_INTERVAL.isLessThanMillis(now - cleanupMarkerFile.lastModified())) {
                return;
            }

            final WeakTimeSeriesDirectoryHashKeyVersionLease[] activeLeasesSnapshot = newActiveLeasesSnapshotPurged();
            for (int i = 0; i < activeLeasesSnapshot.length; i++) {
                final TimeSeriesDirectoryHashKeyVersionLease lease = activeLeasesSnapshot[i].get();
                if (lease != null) {
                    final File cleanupLockFile = new File(heartbeatDirectory, "cleanup.lock");
                    try (HeartbeatFileChannelLock lock = new HeartbeatFileChannelLock(cleanupLockFile)) {
                        if (lock.tryLock()) {
                            final long lockNow = FDateMillis.nowMillis();
                            // Double check marker file after locking in case another process ran cleanup concurrently
                            if (!cleanupMarkerFile.exists()
                                    || CLEANUP_INTERVAL.isLessThanMillis(lockNow - cleanupMarkerFile.lastModified())) {
                                lease.getParent().getParent().cleanupObsoleteVersions();
                                Files.writeStringToFileIfDifferent(cleanupMarkerFile, String.valueOf(lockNow));
                            }
                        }
                    } catch (final Exception e) {
                        throw new RuntimeException("Failed to execute obsolete version cleanup lock", e);
                    }
                    break;
                }
            }
        }

        private void requestWrite() {
            if (writeScheduled.compareAndSet(false, true)) {
                startHeartbeatExecutorIfNeeded();
                startCleanupExecutorIfNeeded();
                heartbeatExecutor.execute(() -> {
                    writeScheduled.set(false);
                    synchronized (DIRECTORY_CONTEXTS) {
                        touchOrRewriteHeartbeat();
                    }
                });
            }
        }

        public boolean touchOrRewriteHeartbeat() {
            final WeakTimeSeriesDirectoryHashKeyVersionLease[] activeLeasesSnapshot = newActiveLeasesSnapshotPurged();
            if (activeLeasesSnapshot.length == 0) {
                deleteHeartbeat();
                lastActiveLeasesSnapshot = activeLeasesSnapshot;
                return false;
            }
            if (Objects.equals(activeLeasesSnapshot, lastActiveLeasesSnapshot) && heartbeatFile.exists()) {
                if (heartbeatFile.setLastModified(FDateMillis.nowMillis())) {
                    return true;
                }
            }
            return writeHeartbeat(activeLeasesSnapshot);
        }

        private WeakTimeSeriesDirectoryHashKeyVersionLease[] newActiveLeasesSnapshotPurged() {
            synchronized (activeLeases) {
                WeakTimeSeriesDirectoryHashKeyVersionLease[] activeLeasesSnapshot = activeLeases
                        .asValueArray(WeakTimeSeriesDirectoryHashKeyVersionLease.EMPTY_ARRAY);
                for (int tries = 0; tries < 10; tries++) {
                    boolean removed = false;
                    for (int i = 0; i < activeLeasesSnapshot.length; i++) {
                        final WeakTimeSeriesDirectoryHashKeyVersionLease lease = activeLeasesSnapshot[i];
                        if (lease.get() == null) {
                            activeLeases.remove(lease.getRegistryKey());
                            removed = true;
                        }
                    }
                    if (removed) {
                        activeLeasesSnapshot = activeLeases
                                .asValueArray(WeakTimeSeriesDirectoryHashKeyVersionLease.EMPTY_ARRAY);
                    } else {
                        break;
                    }
                }
                return activeLeasesSnapshot;
            }
        }

        private boolean writeHeartbeat(final WeakTimeSeriesDirectoryHashKeyVersionLease[] activeLeasesSnapshot) {
            final StringBuilder content = new StringBuilder();

            content.append("Hostname=")
                    .append(IntegrationProperties.HOSTNAME)
                    .append(";ProcessId=")
                    .append(DynamicInstrumentationProperties.getProcessId())
                    .append(";ProcessName=")
                    .append(DynamicInstrumentationProperties.getProcessName())
                    .append(";CreatedTimestamp=")
                    .append(createdTimestamp)
                    .append(";UpdatedTimestamp=")
                    .append(FDate.now());

            for (final WeakTimeSeriesDirectoryHashKeyVersionLease lease : activeLeasesSnapshot) {
                if (lease.get() == null) {
                    synchronized (activeLeases) {
                        activeLeases.remove(lease.getRegistryKey());
                    }
                    continue;
                }
                content.append("\n");
                content.append(lease.getRegistryKey());
            }

            Files.writeStringToFileIfDifferent(heartbeatFile, content.toString());
            lastActiveLeasesSnapshot = activeLeasesSnapshot;
            return true;
        }

        private void deleteHeartbeat() {
            Files.deleteQuietly(heartbeatFile);
        }
    }

    private static final class WeakTimeSeriesDirectoryHashKeyVersionLease
            extends WeakReference<TimeSeriesDirectoryHashKeyVersionLease> {
        public static final WeakTimeSeriesDirectoryHashKeyVersionLease[] EMPTY_ARRAY = new WeakTimeSeriesDirectoryHashKeyVersionLease[0];
        private final String registryKey;

        private WeakTimeSeriesDirectoryHashKeyVersionLease(final TimeSeriesDirectoryHashKeyVersionLease referent) {
            super(referent);
            this.registryKey = referent.getRegistryKey();
        }

        public String getRegistryKey() {
            return registryKey;
        }
    }
}