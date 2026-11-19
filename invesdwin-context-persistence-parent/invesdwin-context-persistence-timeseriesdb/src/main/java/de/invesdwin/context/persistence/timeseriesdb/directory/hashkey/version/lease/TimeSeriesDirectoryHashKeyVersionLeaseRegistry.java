package de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.lease;

import de.invesdwin.context.log.Log;
import java.util.List;
import java.nio.file.DirectoryStream;
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
import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.ITimeSeriesDirectoryHashKey;
import de.invesdwin.context.persistence.timeseriesdb.updater.TimeSeriesEpochReclaim;
import de.invesdwin.instrument.DynamicInstrumentationProperties;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.fast.IFastIterableMap;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.lock.file.AtomicNioFileChannelContext;
import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLock;
import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLockRegistry;
import de.invesdwin.util.error.RuntimeIOException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.string.Charsets;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.millis.FDateMillis;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public final class TimeSeriesDirectoryHashKeyVersionLeaseRegistry {

    private static final Log LOG = new Log(TimeSeriesDirectoryHashKeyVersionLeaseRegistry.class);

    private static final Map<File, SharedDirectoryLeaseContext> DIRECTORY_CONTEXTS = ILockCollectionFactory
            .getInstance(true)
            .newConcurrentMap();
    private static final Duration HEARTBEAT_INTERVAL = Duration.ONE_MINUTE;
    private static final Duration VERSION_CLEANUP_CHECK_INTERVAL = Duration.ONE_HOUR;
    private static final Duration VERSION_CLEANUP_INTERVAL = AtomicNioFileChannelContext.TMP_CLEANUP_INTERVAL;
    private static final String VERSION_CLEANUP_MARKER_FILENAME = ".version"
            + AtomicNioFileChannelContext.CLEANUP_MARKER_EXTENSION;
    private static final long UNINITIALIZED_DIRECTORY_CLEANUP_TIME = AtomicNioFileChannelContext.UNINITIALIZED_DIRECTORY_CLEANUP_TIME;

    private static ScheduledExecutorService heartbeatExecutor;
    private static ScheduledExecutorService cleanupExecutor;
    /**
     * Coalescing of the epoch-reclaim drain: one queued drain is enough, because the drain itself iterates every
     * directory submitted in the meantime.
     */
    private static final AtomicBoolean EPOCH_RECLAIM_SCHEDULED = new AtomicBoolean(false);

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
                }, VERSION_CLEANUP_CHECK_INTERVAL.millisValue(), VERSION_CLEANUP_CHECK_INTERVAL.millisValue(),
                        TimeUnit.MILLISECONDS);
            }
        }
    }

    private static void stopCleanupExecutorIfNeeded() {
        synchronized (TimeSeriesDirectoryHashKeyVersionLeaseRegistry.class) {
            if (DIRECTORY_CONTEXTS.isEmpty() && !TimeSeriesEpochReclaim.hasPendingObsoleteEpochsReclaims()
                    && cleanupExecutor != null && !cleanupExecutor.isShutdown()) {
                cleanupExecutor.shutdown();
                cleanupExecutor = null;
            }
        }
    }

    /**
     * Runs the epoch-reclaim drain on this registry's cleanup thread: one single-threaded janitor for both sweeps
     * (obsolete versions and the attempt-local garbage of finished update attempts), with the lifecycle of the
     * executor already handling start-on-demand and shutdown-on-idle. Not routed through the version cleanup's marker
     * file throttle - that gate is {@link AtomicNioFileChannelContext#TMP_CLEANUP_INTERVAL} (a day), while the epoch
     * retention this sweep obeys is an hour.
     */
    /**
     * Whether some OTHER process currently holds a lease on this hash key's {@code version} directory. The key form
     * is exactly what newRegistryKey() writes into a heartbeat file, so the two sides cannot drift.
     * <p>
     * Read-only on purpose: the registry's only other accessor, getOrCreate(parent, version), would register a lease
     * for this JVM and make us the holder, which is the opposite of the question. A store reset needs to know
     * whether it may delete a retired version's files now, and that is a cross-process fact.
     * <p>
     * A heartbeat whose file is older than {@link HeartbeatFileChannelLockRegistry#HEARTBEAT_TIMEOUT} does not count:
     * a live process re-touches it every {@link HeartbeatFileChannelLockRegistry#HEARTBEAT_INTERVAL}, so an old mtime
     * means its writer crashed and it must not pin the version forever. The version sweep is the fallback for what
     * stays held - not holding a lease is an optimisation of reclaim latency, never a correctness precondition.
     */
    public static boolean isLeasedByForeignProcess(final ITimeSeriesDirectoryHashKey parent, final int version) {
        final File heartbeatsDirectory = parent.getParent().getHeartbeatsDirectory();
        if (!Files.isDirectory(heartbeatsDirectory.toPath())) {
            return false;
        }
        final String registryKey = parent.getHashKey() + "/" + version;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(heartbeatsDirectory.toPath(),
                TimeSeriesDirectoryHashKeyVersionLeaseRegistry::isPublishedHeartbeat)) {
            for (final Path heartbeatFile : stream) {
                if (heartbeatFile.getFileName().toString()
                        .startsWith(Files.normalizeFileName(HeartbeatFileChannelLockRegistry.HEARTBEAT_OWNER))) {
                    //our own leases are not foreign; a reset releases them
                    continue;
                }
                if (!isHeartbeatLive(heartbeatFile.toFile())) {
                    continue;
                }
                if (namesLease(heartbeatFile, registryKey)) {
                    return true;
                }
            }
        } catch (final IOException e) {
            //The directory vanished or became unreadable mid-scan: that is not evidence of "nobody holds it".
            return true;
        }
        return false;
    }

    /**
     * Reading one heartbeat file's lease lines. Fail-closed on purpose: the heartbeat is rewritten in place
     * (writeStringToFileIfDifferent -> truncate), and a lease is registered before the rewrite is scheduled
     * (requestWrite goes to the heartbeat executor), so a live file can be caught empty or half-written. An
     * unreadable or key-less live heartbeat therefore counts as holding, which costs reclaim latency; answering
     * "not held" would cost another process its open payload.
     */
    private static boolean namesLease(final Path heartbeatFile, final String registryKey) {
        final List<String> lines;
        try {
            lines = Files.readAllLines(heartbeatFile);
        } catch (final IOException e) {
            return true;
        }
        boolean leaseLines = false;
        for (final String line : lines) {
            final String trimmed = line.trim();
            if (!trimmed.contains("/")) {
                //the header line (Hostname=/ProcessId=/...) - not a lease
                continue;
            }
            leaseLines = true;
            if (registryKey.equals(trimmed)) {
                return true;
            }
        }
        return !leaseLines;
    }

    /**
     * Whether a heartbeat file was refreshed recently enough to mean its writer is still alive.
     * <p>
     * touchOrRewriteHeartbeat() re-touches the file every HEARTBEAT_INTERVAL even when the lease set did not change,
     * and a JVM that ends up holding no lease deletes its file outright, so a file older than HEARTBEAT_TIMEOUT
     * belongs to a process that died. Nothing reaps those files, so every reader of a heartbeats directory has to
     * ignore them - both this registry's eager check and the version sweep's active-lease set - or a crash pins the
     * versions that process claimed forever.
     */
    /**
     * Only a published heartbeat file counts: an upload stages a temp next to the final name, and a temp is not
     * evidence of a lease - the state it names is either already published or about to be.
     */
    static boolean isPublishedHeartbeat(final Path file) {
        return Files.isRegularFile(file)
                && file.getFileName().toString().endsWith(HeartbeatFileChannelLockRegistry.HEARTBEAT_EXTENSION);
    }

    public static boolean isHeartbeatLive(final File heartbeatFile) {
        return heartbeatFile.lastModified() > FDateMillis.nowMillis()
                - HeartbeatFileChannelLockRegistry.HEARTBEAT_TIMEOUT.millisValue();
    }

    public static void requestEpochReclaim() {
        if (EPOCH_RECLAIM_SCHEDULED.compareAndSet(false, true)) {
            startCleanupExecutorIfNeeded();
            if (cleanupExecutor != null && !cleanupExecutor.isShutdown()) {
                cleanupExecutor.execute(() -> {
                    //cleared before the sweep: a submission made during it queues the next one instead of being lost
                    EPOCH_RECLAIM_SCHEDULED.set(false);
                    TimeSeriesEpochReclaim.runPendingObsoleteEpochsReclaims();
                });
            } else {
                //the executor went away between the start attempt and here; the submission stays pending and the next
                //update attempt for that key asks again
                EPOCH_RECLAIM_SCHEDULED.set(false);
            }
        }
    }

    /**
     * The heartbeat executor's body. Package-visible so a cell can drive one cycle deterministically instead of
     * waiting for HEARTBEAT_INTERVAL (30 s): without that, the retry semantics of a failed publish cannot be tested.
     */
    static void updateHeartbeats() {
        for (final SharedDirectoryLeaseContext context : DIRECTORY_CONTEXTS.values()) {
            boolean held;
            try {
                held = context.touchOrRewriteHeartbeat();
            } catch (final RuntimeException e) {
                //A publish failure must not end the schedule: a store whose heartbeat stops being refreshed reads as
                //dead to the version sweep, which is worse than one publish lost.
                LOG.error("Heartbeat refresh failed for [%s]: %s", context.getHeartbeatsDirectory(), e.getMessage());
                held = true;
            }
            if (!held) {
                DIRECTORY_CONTEXTS.computeIfPresent(context.getHeartbeatsDirectory(),
                        (dir, ctx) -> ctx.isEmpty() ? null : ctx);
            }
        }
        stopHeartbeatExecutorIfNeeded();
        stopCleanupExecutorIfNeeded();
    }

    private static final class SharedDirectoryLeaseContext {
        private final File heartbeatsDirectory;
        private final File heartbeatFile;
        private final Path versionCleanupMarkerPath;
        private final Path tempVersionCleanupMarkerPath;
        private final File versionCleanupLockFile;
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
            this.heartbeatFile = new File(heartbeatsDirectory,
                    Files.setExtensionNormalizeFileName(HeartbeatFileChannelLockRegistry.HEARTBEAT_OWNER,
                            HeartbeatFileChannelLockRegistry.HEARTBEAT_EXTENSION));
            try {
                Files.forceMkdirParent(heartbeatFile);
            } catch (final IOException e) {
                throw new RuntimeIOException(e);
            }
            this.versionCleanupMarkerPath = new File(heartbeatsDirectory, VERSION_CLEANUP_MARKER_FILENAME).toPath();
            this.tempVersionCleanupMarkerPath = versionCleanupMarkerPath.resolveSibling(
                    Files.setExtensionNormalizeFileName(versionCleanupMarkerPath.getFileName().toString(),
                            AtomicNioFileChannelContext.TMP_SUFFIX));
            try {
                Files.createDirectories(tempVersionCleanupMarkerPath.getParent());
            } catch (final IOException e) {
                throw new RuntimeIOException(e);
            }
            this.versionCleanupLockFile = new File(heartbeatsDirectory,
                    Files.normalizeFileName(VERSION_CLEANUP_MARKER_FILENAME + ".lock"));
            try {
                Files.forceMkdirParent(heartbeatFile);
            } catch (final IOException e) {
                throw new RuntimeIOException(e);
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

            //A lease is a claim only once other processes can read it: publishing here, not only through
            //requestWrite(), closes the window where a reset on another node asks whether a foreign heartbeat names
            //this version while this JVM has leased it and not yet announced it. Same monitor the heartbeat task
            //takes, so no scheduled writer can land an older snapshot after this one.
            synchronized (DIRECTORY_CONTEXTS) {
                touchOrRewriteHeartbeat();
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
            if (VERSION_CLEANUP_INTERVAL.isGreaterThanMillis(now - last)) {
                return;
            }

            if (Files.exists(versionCleanupMarkerPath)) {
                final long fileModified = Files.lastModifiedNoThrow(versionCleanupMarkerPath);
                if (fileModified > last) {
                    lastCleanupTime.set(fileModified);
                    last = fileModified;
                }
                if (VERSION_CLEANUP_INTERVAL.isGreaterThanMillis(now - last)) {
                    return;
                }
            }

            final WeakTimeSeriesDirectoryHashKeyVersionLease[] activeLeasesSnapshot = newActiveLeasesSnapshotPurged();
            for (int i = 0; i < activeLeasesSnapshot.length; i++) {
                final TimeSeriesDirectoryHashKeyVersionLease lease = activeLeasesSnapshot[i].get();
                if (lease != null) {
                    try (HeartbeatFileChannelLock lock = new HeartbeatFileChannelLock(versionCleanupLockFile)) {
                        if (lock.tryLock()) {
                            final FDate lockNow = FDate.now();

                            // Re-verify file timestamp after lock acquisition for multi-process safety
                            final long currentFileModified = Files.exists(versionCleanupMarkerPath)
                                    ? Files.lastModifiedNoThrow(versionCleanupMarkerPath)
                                    : 0L;
                            if (currentFileModified > last) {
                                lastCleanupTime.set(currentFileModified);
                                last = currentFileModified;
                            }

                            if (!Files.exists(versionCleanupMarkerPath)
                                    || VERSION_CLEANUP_INTERVAL.isLessThanMillis(lockNow.millisValue() - last)) {
                                lease.cleanupObsoleteVersions();
                                Files.write(tempVersionCleanupMarkerPath,
                                        lockNow.toString().getBytes(Charsets.defaultCharset()));
                                Files.move(tempVersionCleanupMarkerPath, versionCleanupMarkerPath,
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
            if (Files.exists(versionCleanupMarkerPath)) {
                return Files.lastModifiedNoThrow(versionCleanupMarkerPath);
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
            if (VERSION_CLEANUP_INTERVAL.isGreaterThanMillis(now - last)) {
                return;
            }

            if (Files.exists(versionCleanupMarkerPath)) {
                final long fileModified = Files.lastModifiedNoThrow(versionCleanupMarkerPath);
                if (fileModified > last) {
                    lastCleanupTime.set(fileModified);
                    last = fileModified;
                }
                if (VERSION_CLEANUP_INTERVAL.isGreaterThanMillis(now - last)) {
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

            if (!publishHeartbeat(content.toString())) {
                //Deliberately not advancing lastActiveLeasesSnapshot here: touchOrRewriteHeartbeat() answers an
                //unchanged lease set by only re-touching the mtime, so an advanced snapshot after a failed publish
                //would keep the file looking alive while the leases it lists stay the old ones - unannounced
                //indefinitely rather than until the next lease change. Leaving it behind forces a full rewrite next
                //cycle (HEARTBEAT_INTERVAL, 30s).
                return true;
            }
            lastActiveLeasesSnapshot = activeLeasesSnapshot;
            return true;
        }

        /**
         * Staged temp plus rename, so a concurrent reader sees either the whole previous set of leases or the whole
         * new one - never a truncated file. For the truncate-in-place form the measurement is in util's
         * Files.moveAtomic javadoc: 15,913 of 29,587 observations found the name missing with a plain replace move,
         * 0 with this one.
         */
        /**
         * @return whether the file now holds {@code content} - published, or already identical.
         */
        private boolean publishHeartbeat(final String content) {
            String existing = null;
            try {
                existing = Files.readFileToStringNoThrow(heartbeatFile, Charsets.defaultCharset());
            } catch (final RuntimeException e) {
                //readFileToStringNoThrow rethrows despite its name (util Files:865-871 wraps the IOException in a
                //RuntimeException): a missing or unreadable current file simply means "publish it".
            }
            if (content.equals(existing)) {
                return true;
            }
            //One staging name per (directory, process): AtomicNioFileChannelContext.TMP_SUFFIX embeds the heartbeat
            //owner, so two JVMs sweeping one store never write onto each other's staging file.
            final File stagingFile = new File(heartbeatsDirectory,
                    heartbeatFile.getName() + AtomicNioFileChannelContext.TMP_SUFFIX);
            try {
                Files.writeStringToFileIfDifferent(stagingFile, content);
                Files.moveAtomic(stagingFile.toPath(), heartbeatFile.toPath());
                return true;
            } catch (final IOException e) {
                Files.deleteQuietly(stagingFile);
                //A heartbeat that cannot be published makes this JVM's leases invisible to another process's reclaim
                //decision, so a lost publish is worth an error record rather than a debug line. It must not end the
                //lease registration: a process that cannot announce its leases is still a process holding them.
                LOG.error("Unable to publish heartbeat file [%s]: %s", heartbeatFile, e.getMessage());
                return false;
            }
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