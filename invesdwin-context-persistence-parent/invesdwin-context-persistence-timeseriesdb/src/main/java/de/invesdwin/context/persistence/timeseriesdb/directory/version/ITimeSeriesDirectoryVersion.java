package de.invesdwin.context.persistence.timeseriesdb.directory.version;

import java.io.File;

import de.invesdwin.context.persistence.timeseriesdb.directory.ITimeSeriesDirectory;

public interface ITimeSeriesDirectoryVersion {

    ITimeSeriesDirectory getParent();

    File getDirectoryVersionShared();

    File getDirectoryVersionPerNode();

    void delete();

}
