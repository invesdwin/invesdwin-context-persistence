package de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.lease;

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
import de.invesdwin.util.time.date.millis.FDateMillis;

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
