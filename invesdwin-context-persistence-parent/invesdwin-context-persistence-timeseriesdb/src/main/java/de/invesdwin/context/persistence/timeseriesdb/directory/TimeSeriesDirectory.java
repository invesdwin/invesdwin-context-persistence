package de.invesdwin.context.persistence.timeseriesdb.directory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.util.collections.factory.pool.set.ICloseableSet;
import de.invesdwin.util.collections.factory.pool.set.PooledSet;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.time.date.millis.FDateMillis;

@Immutable
public class TimeSeriesDirectory implements ITimeSeriesDirectory {

    private static final String HEARTBEATS_FOLDER_NAME = "heartbeats";

    private final ITimeSeriesBaseDirectory parent;
    private final String storageName;
    private final File directoryShared;
    private final File directoryPerNode;
    private final File heartbeatsDirectory;

    public TimeSeriesDirectory(final ITimeSeriesBaseDirectory parent, final String storageName) {
        this.parent = parent;
        this.storageName = storageName;
        this.directoryShared = new File(parent.getBaseDirectoryShared(), storageName);
        this.directoryPerNode = new File(parent.getBaseDirectoryPerNode(), storageName);
        this.heartbeatsDirectory = new File(directoryShared, HEARTBEATS_FOLDER_NAME);
    }

    @Override
    public ITimeSeriesBaseDirectory getParent() {
        return parent;
    }

    @Override
    public String getStorageName() {
        return storageName;
    }

    @Override
    public File getDirectoryShared() {
        return directoryShared;
    }

    @Override
    public File getDirectoryPerNode() {
        return directoryPerNode;
    }

    @Override
    public File getHeartbeatsDirectory() {
        return heartbeatsDirectory;
    }

    /**
     * Scans heartbeat files and deletes all version directories that are currently not held by any active lease. To
     * prevent race conditions during version migration, it opportunistically preserves the highest stable version
     * (older than 1 hour), any intermediate newer versions, and the absolute maximum version. Unleased directories are
     * only deleted if they are older than 1 hour.
     */
    @Override
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
                        .filter(p -> !sharedPath.relativize(p).startsWith(HEARTBEATS_FOLDER_NAME))
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

    @Override
    public void deleteCorruptedStorage() {
        Files.deleteNative(directoryShared);
        if (!Objects.equals(directoryShared, directoryPerNode)) {
            Files.deleteNative(directoryPerNode);
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(directoryShared.getAbsolutePath()).toString();
    }

}