package de.invesdwin.context.persistence.timeseriesdb.directory.version;

import java.io.File;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.directory.ITimeSeriesDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.version.lock.TimeSeriesDirectoryVersionLease;
import de.invesdwin.context.persistence.timeseriesdb.directory.version.lock.TimeSeriesDirectoryVersionLeaseRegistry;
import de.invesdwin.util.lang.finalizer.AFinalizer;

@ThreadSafe
public class TimeSeriesDirectoryVersion implements ITimeSeriesDirectoryVersion {

    private final ITimeSeriesDirectory parent;
    private final TimeSeriesDirectoryVersionFinalizer finalizer;

    public TimeSeriesDirectoryVersion(final ITimeSeriesDirectory parent, final String version) {
        this.parent = parent;
        this.finalizer = new TimeSeriesDirectoryVersionFinalizer();
        if (version != null) {
            finalizer.lease = TimeSeriesDirectoryVersionLeaseRegistry.getOrCreate(parent, version);
            this.finalizer.register(this);
        }
    }

    @Override
    public ITimeSeriesDirectory getParent() {
        return parent;
    }

    public String getVersion() {
        return getLease().getVersion();
    }

    @Override
    public File getDirectoryVersionShared() {
        return getLease().getDirectoryVersionShared();
    }

    @Override
    public File getDirectoryVersionPerNode() {
        return getLease().getDirectoryVersionPerNode();
    }

    @Override
    public void delete() {
        final TimeSeriesDirectoryVersionLease leaseCopy = finalizer.lease;
        if (leaseCopy != null) {
            leaseCopy.delete();
        }
    }

    private TimeSeriesDirectoryVersionLease getLease() {
        if (finalizer.lease == null) {
            synchronized (this) {
                if (finalizer.lease == null) {
                    final String resolvedVersion = resolveCurrentVersionString(parent);
                    // Use the registry to prevent duplicate locks across instances
                    finalizer.lease = TimeSeriesDirectoryVersionLeaseRegistry.getOrCreate(parent, resolvedVersion);
                    finalizer.register(this);
                }
            }
        }
        return finalizer.lease;
    }

    /**
     * Atomically creates the next incremental version directory and updates this instance to point to it.
     */
    public void incrementVersion() {
        synchronized (this) {
            final TimeSeriesDirectoryVersionLease prevLease = finalizer.lease;
            final String nextVersionStr = electNextVersionString(parent.getDirectoryShared());
            finalizer.lease = TimeSeriesDirectoryVersionLeaseRegistry.getOrCreate(parent, nextVersionStr);
            if (prevLease != null) {
                prevLease.close();
            }
        }
    }

    /**
     * Resolves the active directory version string or atomically creates the next incremental version if none exists.
     */
    private static String resolveCurrentVersionString(final ITimeSeriesDirectory parent) {
        final File sharedDir = parent.getDirectoryShared();
        if (!sharedDir.exists()) {
            sharedDir.mkdirs();
        }

        final long maxVersion = findMaxExistingVersion(sharedDir);

        // If a version already exists, attach to the latest established version
        if (maxVersion > 0) {
            return String.valueOf(maxVersion);
        }

        // If no version exists yet, race atomically to initialize version 1
        return electNextVersionString(sharedDir);
    }

    /**
     * Shared atomic election loop that guarantees only one JVM creates a given directory version number.
     */
    private static String electNextVersionString(final File sharedDir) {
        if (!sharedDir.exists()) {
            sharedDir.mkdirs();
        }

        long maxVersion = findMaxExistingVersion(sharedDir);

        while (true) {
            final long candidateVersion = maxVersion + 1;
            final String candidateStr = String.valueOf(candidateVersion);
            final File candidateDir = new File(sharedDir, candidateStr);

            // Atomic filesystem operation across cluster nodes
            if (candidateDir.mkdir()) {
                return candidateStr; // This JVM won the election
            }

            // Lost the race; re-scan to adopt the version established by the winning node
            maxVersion = findMaxExistingVersion(sharedDir);

            // If called from electNextVersionString, we strictly want a *new* highest version.
            // If another process just created it, we loop again to try maxVersion + 1.
        }
    }

    private static long findMaxExistingVersion(final File sharedDir) {
        final File[] files = sharedDir.listFiles(File::isDirectory);
        long max = 0;
        if (files != null) {
            for (final File file : files) {
                try {
                    final long v = Long.parseLong(file.getName());
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
    public static TimeSeriesDirectoryVersion createCurrentVersion(final ITimeSeriesDirectory parent) {
        final String currentVersionStr = resolveCurrentVersionString(parent);
        return new TimeSeriesDirectoryVersion(parent, currentVersionStr);
    }

    /**
     * Atomically creates and returns a brand-new directory version object for backadjustments/rewrites.
     */
    public static TimeSeriesDirectoryVersion createNextVersion(final ITimeSeriesDirectory parent) {
        final String nextVersionStr = electNextVersionString(parent.getDirectoryShared());
        return new TimeSeriesDirectoryVersion(parent, nextVersionStr);
    }

    private static final class TimeSeriesDirectoryVersionFinalizer extends AFinalizer {

        private volatile TimeSeriesDirectoryVersionLease lease;

        @Override
        protected void clean() {
            final TimeSeriesDirectoryVersionLease leaseCopy = lease;
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

}