package de.invesdwin.context.persistence.timeseriesdb.directory.version.data;

import java.io.File;

import de.invesdwin.context.persistence.timeseriesdb.directory.version.ITimeSeriesDirectoryVersion;
import de.invesdwin.context.system.properties.ICloseableProperties;

public interface ITimeSeriesDirectoryVersionData {

    ITimeSeriesDirectoryVersion getParent();

    String getStorageKey();

    File getDirectoryVersionDataShared();

    File getDirectoryVersionDataPerNode();

    void delete();

    ICloseableProperties getProperties();

}
