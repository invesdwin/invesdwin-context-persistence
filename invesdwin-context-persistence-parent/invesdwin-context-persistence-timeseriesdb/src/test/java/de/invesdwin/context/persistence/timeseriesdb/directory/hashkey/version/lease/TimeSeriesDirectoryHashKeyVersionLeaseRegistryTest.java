package de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.lease;

import de.invesdwin.util.lang.string.Charsets;
import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.context.persistence.timeseriesdb.directory.TimeSeriesDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.TimeSeriesDirectoryHashKey;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLockRegistry;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.date.millis.FDateMillis;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Whether a retired version directory may be deleted now or has to be left to another process.
 * <p>
 * A store reset increments the version and then retires the version it just left. Deleting a directory another live
 * process still reads would take its open files away, so the reset asks this question first: does a heartbeat file in
 * this store's heartbeats directory name this version, written by a process that is not us and still alive. A
 * heartbeat is refreshed every HEARTBEAT_INTERVAL (30s) and times out after HEARTBEAT_TIMEOUT (2 minutes), so an old
 * mtime means the writer crashed - not waiting on it is what keeps a dead process from pinning a version forever,
 * while the version sweep stays the fallback for everything else.
 * <p>
 * Each cell has its own store directory: the cells plant heartbeat files, and a planted file that another cell could
 * read would make the result depend on method order rather than on the rule under test.
 */
@NotThreadSafe
public class TimeSeriesDirectoryHashKeyVersionLeaseRegistryTest extends ATest {

    private static final int VERSION = 7;
    private static final String HASH_KEY = "LEASETESTTABLE/segmentedKey";
    /** Observation windows for the progress assertion - a stalled pair reports the same count twice. */
    private static final long SAMPLE_MILLIS = 3000L;
    private static final long JOIN_MILLIS = 5000L;

    @Test
    public void testAForeignLiveLeaseOfThisVersionPinsIt() throws IOException {
        final File store = store("pinsIt");
        plantHeartbeat(foreignHeartbeatFile(store), registryKey(VERSION));
        Assertions.assertThat(TimeSeriesDirectoryHashKeyVersionLeaseRegistry.isLeasedByForeignProcess(newHashKey(store),
                VERSION)).isTrue();
    }

    @Test
    public void testAStaleForeignLeaseDoesNotPinIt() throws IOException {
        final File store = store("staleForeign");
        final File heartbeatFile = plantHeartbeat(foreignHeartbeatFile(store), registryKey(VERSION));
        Assertions.assertThat(heartbeatFile.setLastModified(FDateMillis.nowMillis()
                - HeartbeatFileChannelLockRegistry.HEARTBEAT_TIMEOUT.millisValue() - 60000L)).isTrue();
        Assertions.assertThat(TimeSeriesDirectoryHashKeyVersionLeaseRegistry.isLeasedByForeignProcess(newHashKey(store),
                VERSION)).isFalse();
    }

    @Test
    public void testOurOwnLeaseIsNotForeign() throws IOException {
        final File store = store("ownLease");
        plantHeartbeat(ourHeartbeatFile(store), registryKey(VERSION));
        Assertions.assertThat(TimeSeriesDirectoryHashKeyVersionLeaseRegistry.isLeasedByForeignProcess(newHashKey(store),
                VERSION)).isFalse();
    }

    @Test
    public void testAForeignLeaseOnAnotherVersionDoesNotPinThisOne() throws IOException {
        final File store = store("otherVersion");
        plantHeartbeat(foreignHeartbeatFile(store), registryKey(VERSION + 1));
        Assertions.assertThat(TimeSeriesDirectoryHashKeyVersionLeaseRegistry.isLeasedByForeignProcess(newHashKey(store),
                VERSION)).isFalse();
    }

    /**
     * The heartbeat file is rewritten in place (truncate, then write) and a lease is registered before that rewrite is
     * scheduled, so a live process can be caught with an unreadable or key-less file. Both answer "held": the cost is
     * reclaim latency, while answering "not held" would cost that process its open payload.
     */
    @Test
    public void testALiveHeartbeatThatCannotBeReadCountsAsHeld() throws IOException {
        final File store = store("unreadable");
        plantHeartbeat(foreignHeartbeatFile(store), registryKey(VERSION + 1));
        Assertions.assertThat(TimeSeriesDirectoryHashKeyVersionLeaseRegistry.isLeasedByForeignProcess(newHashKey(store),
                VERSION)).as("readable, and it names another version").isFalse();
        Assumptions.assumeTrue(foreignHeartbeatFile(store).setReadable(false),
                "this filesystem lets us take read permission away");
        Assertions.assertThat(TimeSeriesDirectoryHashKeyVersionLeaseRegistry.isLeasedByForeignProcess(newHashKey(store),
                VERSION)).as("same file, now unreadable").isTrue();
    }

    @Test
    public void testALiveHeartbeatWithoutLeaseLinesCountsAsHeld() throws IOException {
        final File store = store("keyless");
        plantHeartbeat(foreignHeartbeatFile(store));
        Assertions.assertThat(TimeSeriesDirectoryHashKeyVersionLeaseRegistry.isLeasedByForeignProcess(newHashKey(store),
                VERSION)).isTrue();
    }

    /**
     * The claim only exists once another process can read it: getOrCreate publishes this JVM's heartbeat before it
     * returns, so a reset running on another node asking about this version is not answering from a state that is in
     * this map but not on disk.
     */
    @Test
    public void testANewLeaseIsPublishedBeforeGetOrCreateReturns() throws IOException {
        final File store = store("publishBeforeReturn");
        final TimeSeriesDirectoryHashKey key = newHashKey(store);
        final TimeSeriesDirectoryHashKeyVersionLease lease = TimeSeriesDirectoryHashKeyVersionLeaseRegistry
                .getOrCreate(key, VERSION);
        try {
            final String content = Files.readFileToString(ourHeartbeatFile(store),
                    Charsets.defaultCharset());
            Assertions.assertThat(content).contains(registryKey(VERSION));
        } finally {
            lease.close();
        }
    }

    /**
     * The upload stages a temp next to the final name; a reader that counted it would double-count a state that is
     * either already published or about to be.
     */
    @Test
    public void testAStagedHeartbeatIsNotReadAsALease() throws IOException {
        final File store = store("staged");
        final File staged = new File(heartbeatsDirectory(store),
                "999999@somehost_00000000-0000-0000-0000-000000000000"
                        + HeartbeatFileChannelLockRegistry.HEARTBEAT_EXTENSION + ".tmp");
        plantHeartbeat(staged, registryKey(VERSION));
        Assertions.assertThat(TimeSeriesDirectoryHashKeyVersionLeaseRegistry.isLeasedByForeignProcess(newHashKey(store),
                VERSION)).isFalse();
    }

    /**
     * What a failed heartbeat publish actually costs, and that it is recovered: the file still holds the previous
     * lease set, so the process looks alive with a claim nobody can see. The retry comes from the next heartbeat
     * cycle (HEARTBEAT_INTERVAL), which is only true if a failed publish leaves the last-published snapshot alone -
     * advancing it would make the cycle take the mtime-only branch and never re-announce the missing lease.
     */
    @Test
    public void testAFailedPublishIsRetriedByTheNextHeartbeatCycle() throws IOException {
        final File store = store("publishRetry");
        final TimeSeriesDirectoryHashKey key = newHashKey(store);
        final TimeSeriesDirectoryHashKeyVersionLease first = TimeSeriesDirectoryHashKeyVersionLeaseRegistry
                .getOrCreate(key, VERSION);
        Assertions.assertThat(Files.readFileToString(ourHeartbeatFile(store), Charsets.defaultCharset()))
                .as("the first lease is announced").contains(registryKey(VERSION));

        Assumptions.assumeTrue(heartbeatsDirectory(store).setWritable(false),
                "this filesystem lets us take write permission away");
        final TimeSeriesDirectoryHashKeyVersionLease second;
        try {
            second = TimeSeriesDirectoryHashKeyVersionLeaseRegistry.getOrCreate(key, VERSION + 1);
        } finally {
            heartbeatsDirectory(store).setWritable(true);
        }
        Assertions.assertThat(Files.readFileToString(ourHeartbeatFile(store), Charsets.defaultCharset()))
                .as("the failed publish left the previous content in place")
                .doesNotContain(registryKey(VERSION + 1));

        TimeSeriesDirectoryHashKeyVersionLeaseRegistry.updateHeartbeats();

        Assertions.assertThat(Files.readFileToString(ourHeartbeatFile(store), Charsets.defaultCharset()))
                .as("the next cycle re-publishes the lease that could not be announced")
                .contains(registryKey(VERSION + 1));
        second.close();
        first.close();
    }

    @Test
    public void testAMissingHeartbeatsDirectoryIsNotHeld() {
        final File store = store("noHeartbeats");
        Assertions.assertThat(TimeSeriesDirectoryHashKeyVersionLeaseRegistry.isLeasedByForeignProcess(newHashKey(store),
                VERSION)).isFalse();
    }

    /**
     * The other reader of the same heartbeats directory: a version sweep that treated a crashed process's heartbeat as
     * live would never reclaim the versions it names, because nothing reaps those files. Two aged versions plus the
     * current one, with a stale foreign heartbeat naming the oldest - it goes, the stable one is kept as a fallback.
     */
    @Test
    public void testTheVersionSweepIgnoresAStaleForeignLease() throws IOException {
        final File store = store("sweepStale");
        final TimeSeriesDirectoryHashKey key = newHashKey(store);
        final File staleVersion = new File(key.getDirectoryHashKeyShared(), "1");
        final File stableVersion = new File(key.getDirectoryHashKeyShared(), "2");
        final File newestVersion = new File(key.getDirectoryHashKeyShared(), "3");
        Files.forceMkdir(staleVersion);
        Files.forceMkdir(stableVersion);
        Files.forceMkdir(newestVersion);
        final File heartbeatFile = plantHeartbeat(foreignHeartbeatFile(store), registryKey(1));
        Assertions.assertThat(heartbeatFile.setLastModified(FDateMillis.nowMillis()
                - HeartbeatFileChannelLockRegistry.HEARTBEAT_TIMEOUT.millisValue() - 60000L)).isTrue();
        final long aged = FDateMillis.nowMillis() - TimeSeriesProperties.RETAIN_OBSOLETE_VERSIONS_THRESHOLD.millisValue()
                - 60000L;
        Assertions.assertThat(staleVersion.setLastModified(aged)).isTrue();
        Assertions.assertThat(stableVersion.setLastModified(aged)).isTrue();

        final TimeSeriesDirectoryHashKeyVersionLease lease = TimeSeriesDirectoryHashKeyVersionLeaseRegistry
                .getOrCreate(key, 3);
        try {
            lease.cleanupObsoleteVersions();
        } finally {
            lease.close();
        }

        Assertions.assertThat(staleVersion.exists()).as("the stale lease no longer protects it").isFalse();
        Assertions.assertThat(stableVersion.exists()).as("the stable max is kept as a crash fallback").isTrue();
        Assertions.assertThat(newestVersion.exists()).as("the current version is never swept").isTrue();
    }

    private String registryKey(final int version) {
        return HASH_KEY + "/" + version;
    }

    /**
     * Lease close and heartbeat refresh run against each other on the same context map, and neither may stall the other.
     * <p>
     * What this cell actually pins, proven by mutation, is the submit race: {@code requestWrite} submitted to the
     * heartbeat pool outside the lifecycle monitor, while {@code stopHeartbeatExecutorIfNeeded} shuts that pool down
     * inside it as the last lease closes - so a create concurrent with that close submitted to a terminated executor.
     * Moving the submit under the monitor removes the race; putting it back (submit outside, no monitor) reddens this
     * cell with {@code RejectedExecutionException: … rejected from … [Terminated …]}, which is what it throws on a
     * real run too - and there it matters beyond the throw, because the retry layer reads an unmarked runtime
     * exception as a fault rather than a wait.
     * <p>
     * The second fix in this change is not pinned by this cell. The refresh used to hold the map's monitor and then
     * call {@code computeIfPresent} (monitor then bin) while a close ran inside {@code computeIfPresent} and took that
     * same monitor for the heartbeat delete (bin then monitor) - a real inversion that deadlocked a cold
     * derived-store pass for 70 minutes on 2026-11-25 (17 threads blocked, {@code jstack} reporting no
     * {@code Found deadlock}). Reverting that part does NOT redden this cell: the collision window is far narrower
     * than the file-IO cadence of these loops, so the reproduction is the two thread dumps from that run, not a stress
     * assertion. The progress windows here are a smoke test that both loops keep moving, and the reason the heartbeat
     * serialisation now lives on its own lock is stated at {@code HEARTBEAT_WRITE_LOCK}.
     */
    @Test
    public void testCloseAgainstHeartbeatRefreshKeepsMakingProgress() throws Exception {
        final File store = store("closeAgainstRefresh");
        final AtomicBoolean stop = new AtomicBoolean(false);
        final AtomicInteger rounds = new AtomicInteger();
        final AtomicReference<Throwable> closerFailure = new AtomicReference<>();
        final AtomicReference<Throwable> refresherFailure = new AtomicReference<>();

        final Thread closer = new Thread(() -> {
            try {
                while (!stop.get()) {
                    final TimeSeriesDirectoryHashKeyVersionLease lease = TimeSeriesDirectoryHashKeyVersionLeaseRegistry
                            .getOrCreate(newHashKey(store), VERSION);
                    lease.close();
                    rounds.incrementAndGet();
                }
            } catch (final Throwable t) {
                //a loop that dies silently looks identical to a loop that stalls - report it
                closerFailure.set(t);
            }
        }, "lease-close-stress");
        final Thread refresher = new Thread(() -> {
            try {
                while (!stop.get()) {
                    TimeSeriesDirectoryHashKeyVersionLeaseRegistry.updateHeartbeats();
                }
            } catch (final Throwable t) {
                refresherFailure.set(t);
            }
        }, "lease-heartbeat-stress");
        closer.setDaemon(true);
        refresher.setDaemon(true);
        closer.start();
        refresher.start();

        try {
            FTimeUnit.MILLISECONDS.sleep(SAMPLE_MILLIS);
            Assertions.assertThat(closerFailure.get())
                    .as("the close loop threw, so the progress figure below is not a lock measurement")
                    .isNull();
            Assertions.assertThat(refresherFailure.get())
                    .as("the heartbeat refresh loop threw, so the progress figure below is not a lock measurement")
                    .isNull();
            final int first = rounds.get();
            FTimeUnit.MILLISECONDS.sleep(SAMPLE_MILLIS);
            final int second = rounds.get();
            Assertions.assertThat(second)
                    .as("no lease was closed in the second " + SAMPLE_MILLIS + " ms window (first=" + first
                            + ", second=" + second + ") - the monitor/bin inversion between the heartbeat refresh and"
                            + " lease close is back; jstack does not report it as a deadlock, so look for threads"
                            + " blocked on DIRECTORY_CONTEXTS while another holds a ConcurrentHashMap bin")
                    .isGreaterThan(first);
        } finally {
            stop.set(true);
            closer.join(JOIN_MILLIS);
            refresher.join(JOIN_MILLIS);
        }
        Assertions.assertThat(closer.isAlive())
                .as("the close loop did not stop after stop was set - it is parked on a lock")
                .isFalse();
        Assertions.assertThat(refresher.isAlive())
                .as("the heartbeat refresh loop did not stop after stop was set - it is parked on a lock")
                .isFalse();
    }

    private File store(final String name) {
        return new File(new File(ContextProperties.TEMP_DIRECTORY, "lease-registry"), name);
    }

    private File foreignHeartbeatFile(final File store) {
        return new File(heartbeatsDirectory(store), "999999@somehost_00000000-0000-0000-0000-000000000000"
                + HeartbeatFileChannelLockRegistry.HEARTBEAT_EXTENSION);
    }

    private File ourHeartbeatFile(final File store) {
        return new File(heartbeatsDirectory(store), Files.setExtensionNormalizeFileName(
                HeartbeatFileChannelLockRegistry.HEARTBEAT_OWNER, HeartbeatFileChannelLockRegistry.HEARTBEAT_EXTENSION));
    }

    private File heartbeatsDirectory(final File store) {
        return new File(new File(store, "LEASETEST"), TimeSeriesDirectory.HEARTBEATS_FOLDER_NAME);
    }

    private File plantHeartbeat(final File heartbeatFile, final String... registryKeys) throws IOException {
        Files.forceMkdirParent(heartbeatFile);
        final StringBuilder content = new StringBuilder("Hostname=somehost;ProcessId=999999;ProcessName=test;"
                + "CreatedTimestamp=x;UpdatedTimestamp=x");
        for (final String registryKey : registryKeys) {
            content.append("\n").append(registryKey);
        }
        Files.writeStringToFileIfDifferent(heartbeatFile, content.toString());
        return heartbeatFile;
    }

    private TimeSeriesDirectoryHashKey newHashKey(final File store) {
        final TimeSeriesDirectory directory = new TimeSeriesDirectory(new TimeSeriesBaseDirectory(store), "LEASETEST");
        return new TimeSeriesDirectoryHashKey(directory, HASH_KEY);
    }
}
