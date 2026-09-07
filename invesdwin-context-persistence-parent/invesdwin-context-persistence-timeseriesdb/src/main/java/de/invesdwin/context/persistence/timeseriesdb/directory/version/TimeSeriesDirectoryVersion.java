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

    public TimeSeriesDirectoryVersion(final ITimeSeriesDirectory parent) {
        this.parent = parent;
        this.finalizer = new TimeSeriesDirectoryVersionFinalizer();
    }

    public TimeSeriesDirectoryVersion(final ITimeSeriesDirectory parent, final int version) {
        this(parent);
        finalizer.lease = TimeSeriesDirectoryVersionLeaseRegistry.getOrCreate(parent, version);
        this.finalizer.register(this);
        //        System.out.println(
        //                "TODO: maybe we also need a registry which hashKey uses which version, so that perNode data can be deleted on a version change? or maybe store the version in perNodeData and clear data when any version changes?");
        // or store all leased versions of a node in a single heartbeat file; the overall storage stores the version inside of the per-node lookup caches; the lookup caches are deleted on any version change (deleteRange vs deleteAll differentiation)
        // versions are only differentiated in the hashKey folder
    }

    @Override
    public ITimeSeriesDirectory getParent() {
        return parent;
    }

    @Override
    public int getVersion() {
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
        //System.out.println("TODO: rework this");
        final TimeSeriesDirectoryVersionLease leaseCopy = finalizer.lease;
        if (leaseCopy != null) {
            leaseCopy.delete();
        }
    }

    private TimeSeriesDirectoryVersionLease getLease() {
        if (finalizer.lease == null) {
            synchronized (this) {
                if (finalizer.lease == null) {
                    final int resolvedVersion = resolveCurrentVersion(parent);
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
            final int nextVersion = electNextVersion(parent.getDirectoryShared());
            finalizer.lease = TimeSeriesDirectoryVersionLeaseRegistry.getOrCreate(parent, nextVersion);
            if (prevLease != null) {
                prevLease.close();
            }
        }
    }

    /**
     * Resolves the active directory version string or atomically creates the next incremental version if none exists.
     */
    private static int resolveCurrentVersion(final ITimeSeriesDirectory parent) {
        final File sharedDir = parent.getDirectoryShared();
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
    public static TimeSeriesDirectoryVersion createCurrentVersion(final ITimeSeriesDirectory parent) {
        final int currentVersionStr = resolveCurrentVersion(parent);
        return new TimeSeriesDirectoryVersion(parent, currentVersionStr);
    }

    /**
     * Atomically creates and returns a brand-new directory version object for backadjustments/rewrites.
     */
    public static TimeSeriesDirectoryVersion createNextVersion(final ITimeSeriesDirectory parent) {
        final int nextVersionStr = electNextVersion(parent.getDirectoryShared());
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