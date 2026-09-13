package de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version;

import java.io.File;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.filechannel.info.path.FileChannelPath;
import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannel;
import de.invesdwin.context.integration.filechannel.nio.atomic.properties.TransactionalFileProperties;
import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.ITimeSeriesDirectoryHashKey;
import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.lease.TimeSeriesDirectoryHashKeyVersionLease;
import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.lease.TimeSeriesDirectoryHashKeyVersionLeaseRegistry;
import de.invesdwin.context.system.properties.ICloseableProperties;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.finalizer.AFinalizer;

@ThreadSafe
public class TimeSeriesDirectoryHashKeyVersion implements ITimeSeriesDirectoryHashKeyVersion {

    private final ITimeSeriesDirectoryHashKey parent;
    private final TimeSeriesDirectoryVersionFinalizer finalizer;
    private volatile AtomicNioFileChannel propertiesPath;

    public TimeSeriesDirectoryHashKeyVersion(final ITimeSeriesDirectoryHashKey parent) {
        this.parent = parent;
        this.finalizer = new TimeSeriesDirectoryVersionFinalizer();
    }

    public TimeSeriesDirectoryHashKeyVersion(final ITimeSeriesDirectoryHashKey parent, final int version) {
        this(parent);
        finalizer.lease = TimeSeriesDirectoryHashKeyVersionLeaseRegistry.getOrCreate(parent, version);
        this.finalizer.register(this);
    }

    @Override
    public ITimeSeriesDirectoryHashKey getParent() {
        return parent;
    }

    @Override
    public int getVersion() {
        return getLease().getVersion();
    }

    @Override
    public File getDirectoryHashKeyVersionShared() {
        return getLease().getDirectoryHashKeyVersionShared();
    }

    @Override
    public File getDirectoryHashKeyVersionPerNode() {
        return getLease().getDirectoryHashKeyVersionPerNode();
    }

    @Override
    public ICloseableProperties getProperties() {
        return new TransactionalFileProperties(this::getPropertiesPath);
    }

    private AtomicNioFileChannel getPropertiesPath() {
        if (propertiesPath == null) {
            synchronized (this) {
                if (propertiesPath == null) {
                    propertiesPath = new AtomicNioFileChannel(FileChannelPath.newFile(new File(
                            new File(getDirectoryHashKeyVersionShared(), "properties"), "version.properties")));
                }
            }
        }
        return propertiesPath;
    }

    private TimeSeriesDirectoryHashKeyVersionLease getLease() {
        if (finalizer.lease == null) {
            synchronized (this) {
                if (finalizer.lease == null) {
                    final int curVersion = resolveCurrentVersion();
                    // Instantiating the lease automatically calls Files.forceMkdir for the version directory
                    finalizer.lease = TimeSeriesDirectoryHashKeyVersionLeaseRegistry.getOrCreate(parent, curVersion);
                    finalizer.register(this);
                }
            }
        }
        return finalizer.lease;
    }

    /**
     * Atomically creates the next incremental version directory and updates this instance to point to it.
     */
    @Override
    public void incrementVersion() {
        synchronized (this) {
            final TimeSeriesDirectoryHashKeyVersionLease prevLease = getLease();
            final int currentVersion = prevLease.getVersion();

            final File sharedDir = parent.getDirectoryHashKeyShared();
            final int maxExistingOnDisk = findMaxExistingVersion(sharedDir);

            if (maxExistingOnDisk > currentVersion) {
                // Adopt the higher version already established by another node
                finalizer.lease = TimeSeriesDirectoryHashKeyVersionLeaseRegistry.getOrCreate(parent, maxExistingOnDisk);
            } else {
                // We are at the highest known version.
                // If our current version is completely empty, reuse it as a clean slate.
                final File currentSharedDir = prevLease.getDirectoryHashKeyVersionShared();
                if (Files.isEmptyDirectory(currentSharedDir)) {
                    return; // Stay on the current version to populate it
                }

                // Strictly increment based on local state to prevent mass-creation of subsequent versions
                final int nextVersion = currentVersion + 1;
                finalizer.lease = TimeSeriesDirectoryHashKeyVersionLeaseRegistry.getOrCreate(parent, nextVersion);
            }

            propertiesPath = null;
            prevLease.close();
        }
    }

    /**
     * Resolves the active directory version string or returns 0 if none exists.
     */
    private int resolveCurrentVersion() {
        final File sharedDir = parent.getDirectoryHashKeyShared();
        final int maxVersion = findMaxExistingVersion(sharedDir);

        // Return 0 if the directory does not exist or is empty.
        // The lease instantiation will handle creating the physical directory.
        return maxVersion >= 0 ? maxVersion : 0;
    }

    private int findMaxExistingVersion(final File sharedDir) {
        final File[] files = sharedDir.listFiles(File::isDirectory);
        int max = -1;
        if (files != null) {
            for (final File file : files) {
                try {
                    final int v = Integer.parseInt(file.getName());
                    if (v > max) {
                        max = v;
                    }
                } catch (final NumberFormatException e) {
                    // Ignore non-numeric version directories
                }
            }
        }
        return max;
    }

    private static final class TimeSeriesDirectoryVersionFinalizer extends AFinalizer {

        private volatile TimeSeriesDirectoryHashKeyVersionLease lease;

        @Override
        protected void clean() {
            final TimeSeriesDirectoryHashKeyVersionLease leaseCopy = lease;
            if (leaseCopy != null) {
                leaseCopy.close();
                lease = null;
            }
        }

        @Override
        protected boolean isCleaned() {
            return lease == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }

    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("version", getVersion()).with(parent).toString();
    }

}