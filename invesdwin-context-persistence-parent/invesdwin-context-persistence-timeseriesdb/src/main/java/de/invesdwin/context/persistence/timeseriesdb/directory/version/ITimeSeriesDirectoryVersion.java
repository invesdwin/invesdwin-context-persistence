package de.invesdwin.context.persistence.timeseriesdb.directory.version;

import java.io.File;

import de.invesdwin.context.persistence.timeseriesdb.directory.ITimeSeriesDirectory;

public interface ITimeSeriesDirectoryVersion {

    ITimeSeriesDirectory getParent();

    int getVersion();

    File getDirectoryVersionShared();

    File getDirectoryVersionPerNode();

    void delete();

}
