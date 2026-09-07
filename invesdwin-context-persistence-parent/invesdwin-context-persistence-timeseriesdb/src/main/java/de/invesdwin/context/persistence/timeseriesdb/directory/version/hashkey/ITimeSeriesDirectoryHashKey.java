package de.invesdwin.context.persistence.timeseriesdb.directory.version.hashkey;

import java.io.File;

import de.invesdwin.context.persistence.timeseriesdb.directory.ITimeSeriesDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.version.hashkey.version.ITimeSeriesDirectoryHashKeyVersion;

public interface ITimeSeriesDirectoryHashKey {

    ITimeSeriesDirectory getParent();

    /**
     * The hash key of the data (e.g. instrument/dailyData/segmentKey)
     */
    String getHashKey();

    File getDirectoryHashKeyShared();

    File getDirectoryHashKeyPerNode();

    void delete();

    ITimeSeriesDirectoryHashKeyVersion getDirectoryHashKeyVersion();

}
