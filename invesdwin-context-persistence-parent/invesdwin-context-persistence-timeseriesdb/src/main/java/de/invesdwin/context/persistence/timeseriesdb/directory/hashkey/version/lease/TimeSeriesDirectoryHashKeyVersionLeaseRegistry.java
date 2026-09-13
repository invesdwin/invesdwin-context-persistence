package de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.lease;

import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannelContext;
import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.ITimeSeriesDirectoryHashKey;
import de.invesdwin.instrument.DynamicInstrumentationProperties;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.fast.IFastIterableMap;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLock;
import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLockRegistry;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.string.Charsets;
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
    private static final Duration CLEANUP_INTERVAL = AtomicNioFileChannelContext.CLEANUP_INTERVAL;
    private static final String CLEANUP_MARKER_FILENAME = AtomicNioFileChannelContext.CLEANUP_MARKER_FILENAME;
    private static final long UNINITIALIZED_DIRECTORY_CLEANUP_TIME = AtomicNioFileChannelContext.UNINITIALIZED_DIRECTORY_CLEANUP_TIME;

    private static ScheduledExecutorService heartbeatExecutor;
    private static ScheduledExecutorService cleanupExecutor;

    private TimeSeriesDirectoryHashKeyVersionLeaseRegistry() {}

    public static TimeSeriesDirectoryHashKeyVersionLease getOrCreate(final ITimeSeriesDirectoryHashKey parent,
            final int version) {

        final File heartbeatsDirectory = parent.getParent().getHeartbeatsDirectory();
        final SharedDirectoryLeaseContext context = DIRECTORY_CONTEXTS.computeIfAbsent(heartbeatsDirectory,
                SharedDirectoryLeaseContext::new);

        return context.getOrCreateLease(parent, version);
    }

    static void remove(final TimeSeriesDirectoryHashKeyVersionLease lease) {
        final File heartbeatsDirectory = lease.getHeartbeatsDirectory();
        DIRECTORY_CONTEXTS.computeIfPresent(heartbeatsDirectory, (dir, context) -> {
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
                }, CLEANUP_CHECK_INTERVAL.millisValue(), CLEANUP_CHECK_INTERVAL.millisValue(), TimeUnit.MILLISECONDS);
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
                DIRECTORY_CONTEXTS.computeIfPresent(context.getHeartbeatsDirectory(),
                        (dir, ctx) -> ctx.isEmpty() ? null : ctx);
            }
        }
        stopHeartbeatExecutorIfNeeded();
        stopCleanupExecutorIfNeeded();
    }

    private static final class SharedDirectoryLeaseContext {
        private final File heartbeatsDirectory;
        private final Path heartbeatsDirectoryPath;
        private final File heartbeatFile;
        private final Path cleanupMarkerPath;
        private final Path tempCleanupMarkerPath;
        private final File cleanupLockFile;
        private final FDate createdTimestamp = FDate.now();

        private final AtomicBoolean writeScheduled = new AtomicBoolean(false);
        private final AtomicBoolean cleanupScheduled = new AtomicBoolean(false);
        private final AtomicLong lastCleanupTime = new AtomicLong(UNINITIALIZED_DIRECTORY_CLEANUP_TIME);

        @GuardedBy("activeLeases")
        private final IFastIterableMap<String, WeakTimeSeriesDirectoryHashKeyVersionLease> activeLeases = ILockCollectionFactory
                .getInstance(false)
                .newFastIterableMap();
        private WeakTimeSeriesDirectoryHashKeyVersionLease[] lastActiveLeasesSnapshot = WeakTimeSeriesDirectoryHashKeyVersionLease.EMPTY_ARRAY;

        private SharedDirectoryLeaseContext(final File heartbeatsDirectory) {
            this.heartbeatsDirectory = heartbeatsDirectory;
            this.heartbeatsDirectoryPath = heartbeatsDirectory.toPath();
            this.heartbeatFile = new File(heartbeatsDirectory,
                    Files.normalizeFilename(HeartbeatFileChannelLockRegistry.HEARTBEAT_OWNER
                            + HeartbeatFileChannelLockRegistry.HEARTBEAT_EXTENSION));
            this.cleanupMarkerPath = new File(heartbeatsDirectory, CLEANUP_MARKER_FILENAME).toPath();
            this.tempCleanupMarkerPath = cleanupMarkerPath.resolveSibling(Files.normalizeFilename(
                    cleanupMarkerPath.getFileName().toString() + AtomicNioFileChannelContext.TMP_SUFFIX));
            this.cleanupLockFile = new File(heartbeatsDirectory, CLEANUP_MARKER_FILENAME + ".lock");
            try {
                Files.forceMkdirParent(heartbeatFile);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

        public File getHeartbeatsDirectory() {
            return heartbeatsDirectory;
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
            requestCleanupCheck();
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
            final long now = FDateMillis.nowMillis();
            long last = resolveLastCleanupTime();

            // In-memory fast exit: skip disk check entirely if memory shows a recent cleanup
            if (CLEANUP_INTERVAL.isGreaterThanMillis(now - last)) {
                return;
            }

            if (Files.exists(cleanupMarkerPath)) {
                final long fileModified = Files.lastModifiedNoThrow(cleanupMarkerPath);
                if (fileModified > last) {
                    lastCleanupTime.set(fileModified);
                    last = fileModified;
                }
                if (CLEANUP_INTERVAL.isGreaterThanMillis(now - last)) {
                    return;
                }
            }

            final WeakTimeSeriesDirectoryHashKeyVersionLease[] activeLeasesSnapshot = newActiveLeasesSnapshotPurged();
            for (int i = 0; i < activeLeasesSnapshot.length; i++) {
                final TimeSeriesDirectoryHashKeyVersionLease lease = activeLeasesSnapshot[i].get();
                if (lease != null) {
                    try (HeartbeatFileChannelLock lock = new HeartbeatFileChannelLock(cleanupLockFile)) {
                        if (lock.tryLock()) {
                            final FDate lockNow = FDate.now();

                            // Re-verify file timestamp after lock acquisition for multi-process safety
                            final long currentFileModified = Files.exists(cleanupMarkerPath)
                                    ? Files.lastModifiedNoThrow(cleanupMarkerPath)
                                    : 0L;
                            if (currentFileModified > last) {
                                lastCleanupTime.set(currentFileModified);
                                last = currentFileModified;
                            }

                            if (!Files.exists(cleanupMarkerPath)
                                    || CLEANUP_INTERVAL.isLessThanMillis(lockNow.millisValue() - last)) {
                                lease.cleanupObsoleteVersions();
                                Files.write(tempCleanupMarkerPath,
                                        lockNow.toString().getBytes(Charsets.defaultCharset()));
                                Files.move(tempCleanupMarkerPath, cleanupMarkerPath,
                                        StandardCopyOption.REPLACE_EXISTING);
                                lastCleanupTime.set(lockNow.millisValue());
                            }
                        }
                    } catch (final Exception e) {
                        throw new RuntimeException("Failed to execute obsolete version cleanup lock", e);
                    }
                    break;
                }
            }
        }

        private long resolveLastCleanupTime() {
            long last = lastCleanupTime.get();
            if (last == UNINITIALIZED_DIRECTORY_CLEANUP_TIME) {
                last = loadInitialCleanupTime();
                lastCleanupTime.compareAndSet(UNINITIALIZED_DIRECTORY_CLEANUP_TIME, last);
                last = lastCleanupTime.get();
            }
            return last;
        }

        private long loadInitialCleanupTime() {
            if (Files.exists(cleanupMarkerPath)) {
                return Files.lastModifiedNoThrow(cleanupMarkerPath);
            }
            return 0L;
        }

        private void requestWrite() {
            if (writeScheduled.compareAndSet(false, true)) {
                startHeartbeatExecutorIfNeeded();
                heartbeatExecutor.execute(() -> {
                    writeScheduled.set(false);
                    synchronized (DIRECTORY_CONTEXTS) {
                        touchOrRewriteHeartbeat();
                    }
                });
            }
        }

        private void requestCleanupCheck() {
            final long now = FDateMillis.nowMillis();
            long last = resolveLastCleanupTime();

            // Skip scheduling if memory indicates cleanup occurred within the last 24 hours
            if (CLEANUP_INTERVAL.isGreaterThanMillis(now - last)) {
                return;
            }

            if (Files.exists(cleanupMarkerPath)) {
                final long fileModified = Files.lastModifiedNoThrow(cleanupMarkerPath);
                if (fileModified > last) {
                    lastCleanupTime.set(fileModified);
                    last = fileModified;
                }
                if (CLEANUP_INTERVAL.isGreaterThanMillis(now - last)) {
                    return;
                }
            }

            if (cleanupScheduled.compareAndSet(false, true)) {
                startCleanupExecutorIfNeeded();
                cleanupExecutor.execute(() -> {
                    cleanupScheduled.set(false);
                    cleanupObsoleteVersions();
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