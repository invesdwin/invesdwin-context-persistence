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

import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.util.collections.factory.pool.set.ICloseableSet;
import de.invesdwin.util.collections.factory.pool.set.PooledSet;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.string.Strings;

@Immutable
public class TimeSeriesDirectory implements ITimeSeriesDirectory {

    private static final String HEARTBEAT_FOLDER_NAME = "heartbeat";
    private final ITimeSeriesBaseDirectory parent;
    private final String storageName;
    private final File directoryShared;
    private final File directoryPerNode;
    private final File heartbeatDirectory;

    public TimeSeriesDirectory(final ITimeSeriesBaseDirectory parent, final String storageName) {
        this.parent = parent;
        this.storageName = storageName;
        this.directoryShared = new File(parent.getBaseDirectoryShared(), storageName);
        this.directoryPerNode = new File(parent.getBaseDirectoryPerNode(), storageName);
        this.heartbeatDirectory = new File(directoryShared, HEARTBEAT_FOLDER_NAME);
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
    public File getHeartbeatDirectory() {
        return heartbeatDirectory;
    }

    /**
     * Scans heartbeat files and deletes all version directories that are currently not held by any active lease, while
     * guaranteeing the highest established version for each hashKey is preserved.
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
                        .filter(p -> !sharedPath.relativize(p).startsWith(HEARTBEAT_FOLDER_NAME))
                        .filter(p -> Strings.isInteger(p.getFileName().toString()))
                        .collect(Collectors.groupingBy(Path::getParent))
                        .forEach((parentDir, versionDirs) -> {

                            // Reconstruct the normalized hashKey path to perfectly match the lease registry
                            final String hashKey = sharedPath.relativize(parentDir).toString().replace('\\', '/');

                            int maxVersion = -1;
                            for (int i = 0; i < versionDirs.size(); i++) {
                                final Path vDir = versionDirs.get(i);
                                try {
                                    final int v = Integer.parseInt(vDir.getFileName().toString());
                                    if (v > maxVersion) {
                                        maxVersion = v;
                                    }
                                } catch (final NumberFormatException e) {
                                    // Ignore non-numeric structures
                                }
                            }

                            for (int i = 0; i < versionDirs.size(); i++) {
                                final Path versionDir = versionDirs.get(i);
                                final String versionStr = versionDir.getFileName().toString();

                                try {
                                    if (Integer.parseInt(versionStr) == maxVersion) {
                                        continue; // Always preserve the latest version
                                    }
                                } catch (final NumberFormatException e) {
                                    // Fall through to lease check if parsing fails
                                }

                                final String registryKey = hashKey + "/" + versionStr;

                                if (!activeLeases.contains(registryKey)) {
                                    Files.deleteNative(versionDir.toFile());

                                    final File perNodeVersionDir = new File(new File(directoryPerNode, hashKey),
                                            versionStr);
                                    if (perNodeVersionDir.exists()
                                            && !Objects.equals(versionDir.toFile(), perNodeVersionDir)) {
                                        Files.deleteNative(perNodeVersionDir);
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
        final Path heartbeatPath = heartbeatDirectory.toPath();
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