package de.invesdwin.context.persistence.timeseriesdb.directory;

import java.io.File;

import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.system.properties.IProperties;

public interface ITimeSeriesDirectory {

    ITimeSeriesBaseDirectory getParent();

    String getStorageName();

    File getDirectoryShared();

    File getDirectoryPerNode();

    File getHeartbeatsDirectory();

    void deleteCorruptedStorage();

    IProperties getStoragePropertiesShared();

    IProperties getStoragePropertiesPerNode();

}
