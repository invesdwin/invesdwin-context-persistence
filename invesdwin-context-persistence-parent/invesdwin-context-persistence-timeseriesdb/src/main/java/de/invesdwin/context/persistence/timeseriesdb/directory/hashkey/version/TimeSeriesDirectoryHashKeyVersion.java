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
                    final int resolvedVersion = resolveCurrentVersion(parent);
                    // Use the registry to prevent duplicate locks across instances
                    finalizer.lease = TimeSeriesDirectoryHashKeyVersionLeaseRegistry.getOrCreate(parent,
                            resolvedVersion);
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
            final TimeSeriesDirectoryHashKeyVersionLease prevLease = finalizer.lease;
            final int nextVersion = electNextVersion(parent.getDirectoryHashKeyShared());
            finalizer.lease = TimeSeriesDirectoryHashKeyVersionLeaseRegistry.getOrCreate(parent, nextVersion);
            propertiesPath = null;
            if (prevLease != null) {
                prevLease.close();
            }
        }
    }

    /**
     * Resolves the active directory version string or atomically creates the next incremental version if none exists.
     */
    private static int resolveCurrentVersion(final ITimeSeriesDirectoryHashKey parent) {
        final File sharedDir = parent.getDirectoryHashKeyShared();
        if (!sharedDir.exists()) {
            sharedDir.mkdirs();
        }

        final int maxVersion = findMaxExistingVersion(sharedDir);

        // If a version already exists, attach to the latest established version
        if (maxVersion > 0) {
            return maxVersion;
        }

        // If no version exists yet, race atomically to initialize version 1
        return electNextVersion(sharedDir);
    }

    /**
     * Shared atomic election loop that guarantees only one JVM creates a given directory version number.
     */
    private static int electNextVersion(final File sharedDir) {
        if (!sharedDir.exists()) {
            sharedDir.mkdirs();
        }

        int maxVersion = findMaxExistingVersion(sharedDir);

        while (true) {
            final int candidateVersion = maxVersion + 1;
            final File candidateDir = new File(sharedDir, String.valueOf(candidateVersion));

            // Atomic filesystem operation across cluster nodes
            if (candidateDir.mkdir()) {
                return candidateVersion; // This JVM won the election
            }

            // Lost the race; re-scan to adopt the version established by the winning node
            maxVersion = findMaxExistingVersion(sharedDir);

            // If called from electNextVersionString, we strictly want a *new* highest version.
            // If another process just created it, we loop again to try maxVersion + 1.
        }
    }

    private static int findMaxExistingVersion(final File sharedDir) {
        final File[] files = sharedDir.listFiles(File::isDirectory);
        int max = 0;
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

    /**
     * Returns a directory version object pointing to the current highest version. If no version exists yet, it races
     * atomically to initialize version 1.
     */
    public static TimeSeriesDirectoryHashKeyVersion createCurrentVersion(final ITimeSeriesDirectoryHashKey parent) {
        final int currentVersionStr = resolveCurrentVersion(parent);
        return new TimeSeriesDirectoryHashKeyVersion(parent, currentVersionStr);
    }

    /**
     * Atomically creates and returns a brand-new directory version object for backadjustments/rewrites.
     */
    public static TimeSeriesDirectoryHashKeyVersion createNextVersion(final ITimeSeriesDirectoryHashKey parent) {
        final int nextVersionStr = electNextVersion(parent.getDirectoryHashKeyShared());
        return new TimeSeriesDirectoryHashKeyVersion(parent, nextVersionStr);
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