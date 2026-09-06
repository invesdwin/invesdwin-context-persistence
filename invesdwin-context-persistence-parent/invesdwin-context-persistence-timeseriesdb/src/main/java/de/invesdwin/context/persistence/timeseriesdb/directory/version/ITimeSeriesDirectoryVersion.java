package de.invesdwin.context.persistence.timeseriesdb.directory.version;

import java.io.File;

import de.invesdwin.context.persistence.timeseriesdb.directory.ITimeSeriesDirectory;

public interface ITimeSeriesDirectoryVersion {

    ITimeSeriesDirectory getParent();

    /**
     * This is an interned string so that == comparisons can be made.
     */
    String getVersion();

    File getDirectoryVersionShared();

    File getDirectoryVersionPerNode();

    void delete();

}
