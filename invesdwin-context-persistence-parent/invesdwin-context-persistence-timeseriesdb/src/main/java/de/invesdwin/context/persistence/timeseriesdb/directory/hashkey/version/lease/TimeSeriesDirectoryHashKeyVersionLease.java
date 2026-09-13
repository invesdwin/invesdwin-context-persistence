package de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.lease;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.context.persistence.timeseriesdb.directory.TimeSeriesDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.ITimeSeriesDirectoryHashKey;
import de.invesdwin.util.collections.factory.pool.set.ICloseableSet;
import de.invesdwin.util.collections.factory.pool.set.PooledSet;
import de.invesdwin.util.error.RuntimeIOException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.streams.closeable.ISafeCloseable;
import de.invesdwin.util.time.date.millis.FDateMillis;

@ThreadSafe
public final class TimeSeriesDirectoryHashKeyVersionLease implements ISafeCloseable {

    public static final TimeSeriesDirectoryHashKeyVersionLease[] EMPTY_ARRAY = new TimeSeriesDirectoryHashKeyVersionLease[0];

    private final String registryKey;
    private final File directoryShared;
    private final File directoryPerNode;
    private final File heartbeatsDirectory;
    private final File directoryHashKeyVersionShared;
    private final File directoryHashKeyVersionPerNode;
    private final int version;
    private final AtomicInteger refCount = new AtomicInteger(0);

    TimeSeriesDirectoryHashKeyVersionLease(final String registryKey, final ITimeSeriesDirectoryHashKey parent,
            final int version) {
        this.registryKey = registryKey;
        this.directoryShared = parent.getParent().getDirectoryShared();
        this.directoryPerNode = parent.getParent().getDirectoryPerNode();
        this.heartbeatsDirectory = parent.getParent().getHeartbeatsDirectory();
        this.directoryHashKeyVersionShared = new File(parent.getDirectoryHashKeyShared(), String.valueOf(version));
        this.directoryHashKeyVersionPerNode = new File(parent.getDirectoryHashKeyPerNode(), String.valueOf(version));
        this.version = version;

        try {
            Files.forceMkdir(directoryHashKeyVersionShared);
            Files.forceMkdir(directoryHashKeyVersionPerNode);
        } catch (final IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public String getRegistryKey() {
        return registryKey;
    }

    public File getHeartbeatsDirectory() {
        return heartbeatsDirectory;
    }

    public void retain() {
        refCount.incrementAndGet();
    }

    public int getVersion() {
        return version;
    }

    public File getDirectoryHashKeyVersionShared() {
        return directoryHashKeyVersionShared;
    }

    public File getDirectoryHashKeyVersionPerNode() {
        return directoryHashKeyVersionPerNode;
    }

    @Override
    public void close() {
        if (refCount.decrementAndGet() <= 0) {
            TimeSeriesDirectoryHashKeyVersionLeaseRegistry.remove(this);
        }
    }

    public void delete() {
        Files.deleteNative(directoryHashKeyVersionShared);
        if (!Objects.equals(directoryHashKeyVersionShared, directoryHashKeyVersionPerNode)) {
            Files.deleteNative(directoryHashKeyVersionPerNode);
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(directoryHashKeyVersionShared.getAbsolutePath()).toString();
    }

    /**
     * Scans heartbeat files and deletes all version directories that are currently not held by any active lease. To
     * prevent race conditions during version migration, it opportunistically preserves the highest stable version
     * (older than 1 hour), any intermediate newer versions, and the absolute maximum version. Unleased directories are
     * only deleted if they are older than 1 hour.
     */
    public void cleanupObsoleteVersions() {
        final Path sharedPath = directoryShared.toPath();
        if (!Files.exists(sharedPath)) {
            return;
        }

        try (ICloseableSet<String> activeLeases = PooledSet.getInstance()) {
            populateActiveHeartbeatLeases(activeLeases);

            // Directly stick to NIO streams to walk deeply nested, split hashKey subdirectories
            try (Stream<Path> pathStream = Files.walk(sharedPath)) {
                pathStream.filter(Files::isDirectory)
                        .filter(p -> !p.equals(sharedPath))
                        .filter(p -> !sharedPath.relativize(p).startsWith(TimeSeriesDirectory.HEARTBEATS_FOLDER_NAME))
                        .filter(p -> Strings.isInteger(p.getFileName().toString()))
                        .collect(Collectors.groupingBy(Path::getParent))
                        .forEach((parentDir, versionDirs) -> {

                            // Reconstruct the normalized hashKey path to perfectly match the lease registry
                            final String hashKey = sharedPath.relativize(parentDir).toString().replace('\\', '/');

                            int maxVersion = -1;
                            int stableMaxVersion = -1;

                            for (int i = 0; i < versionDirs.size(); i++) {
                                final Path vDir = versionDirs.get(i);
                                try {
                                    final int v = Integer.parseInt(vDir.getFileName().toString());
                                    if (v > maxVersion) {
                                        maxVersion = v;
                                    }

                                    final long ageInMillis = FDateMillis.nowMillis() - vDir.toFile().lastModified();
                                    if (TimeSeriesProperties.RETAIN_OBSOLETE_VERSIONS_THRESHOLD
                                            .isLessThanMillis(ageInMillis) && v > stableMaxVersion) {
                                        stableMaxVersion = v;
                                    }
                                } catch (final NumberFormatException e) {
                                    // Ignore non-numeric structures
                                }
                            }

                            for (int i = 0; i < versionDirs.size(); i++) {
                                final Path versionDir = versionDirs.get(i);
                                final String versionStr = versionDir.getFileName().toString();

                                try {
                                    final int v = Integer.parseInt(versionStr);

                                    if (v == maxVersion) {
                                        continue; // Always preserve absolute latest version
                                    }
                                    if (stableMaxVersion != -1 && v >= stableMaxVersion) {
                                        continue; // Preserve the stable max version and any intermediate newer versions
                                    }
                                } catch (final NumberFormatException e) {
                                    // Fall through to lease check if parsing fails
                                }

                                final String registryKey = hashKey + "/" + versionStr;

                                if (!activeLeases.contains(registryKey)) {
                                    final long ageInMillis = FDateMillis.nowMillis()
                                            - versionDir.toFile().lastModified();

                                    // Only delete if the directory is older than 1 hour
                                    if (TimeSeriesProperties.RETAIN_OBSOLETE_VERSIONS_THRESHOLD
                                            .isLessThanMillis(ageInMillis)) {
                                        Files.deleteNative(versionDir.toFile());

                                        final File perNodeVersionDir = new File(new File(directoryPerNode, hashKey),
                                                versionStr);
                                        if (perNodeVersionDir.exists()
                                                && !Objects.equals(versionDir.toFile(), perNodeVersionDir)) {
                                            Files.deleteNative(perNodeVersionDir);
                                        }
                                    }
                                }
                            }
                        });
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void populateActiveHeartbeatLeases(final Set<String> activeLeases) {
        final Path heartbeatPath = heartbeatsDirectory.toPath();
        if (!Files.isDirectory(heartbeatPath)) {
            return;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(heartbeatPath, Files::isRegularFile)) {
            for (final Path heartbeatFile : stream) {
                try {
                    final List<String> lines = Files.readAllLines(heartbeatFile);
                    for (int i = 1; i < lines.size(); i++) {
                        final String trimmed = lines.get(i).trim();
                        if (!Strings.isBlank(trimmed) && trimmed.contains("/")) {
                            activeLeases.add(trimmed);
                        }
                    }
                } catch (final IOException e) {
                    // Ignore concurrently deleted or modified heartbeat files
                }
            }
        } catch (final IOException e) {
            // Ignore directory stream errors
        }
    }
}