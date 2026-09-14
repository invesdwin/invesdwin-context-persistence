package de.invesdwin.context.persistence.timeseriesdb.directory;

import java.io.File;

import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;

public interface ITimeSeriesDirectory {

    ITimeSeriesBaseDirectory getParent();

    String getStorageName();

    File getDirectoryShared();

    File getDirectoryPerNode();

    File getHeartbeatsDirectory();

    void deleteCorruptedStorage();

}
