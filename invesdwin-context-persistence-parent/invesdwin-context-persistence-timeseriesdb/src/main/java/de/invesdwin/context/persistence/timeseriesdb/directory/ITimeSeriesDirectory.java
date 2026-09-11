package de.invesdwin.context.persistence.timeseriesdb.directory;

import java.io.File;

import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;

public interface ITimeSeriesDirectory {

    ITimeSeriesBaseDirectory getParent();

    String getStorageName();

    File getDirectoryShared();

    File getDirectoryPerNode();

    File getHeartbeatDirectory();

    void deleteCorruptedStorage();

    /**
     * Scans heartbeat files and deletes all version directories that are currently not held by any active lease, while
     * guaranteeing the highest established version for each hashKey is preserved.
     */
    void cleanupObsoleteVersions();

}
