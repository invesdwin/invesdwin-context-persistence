package de.invesdwin.context.persistence.timeseriesdb.directory.hashkey;

import java.io.File;

import de.invesdwin.context.persistence.timeseriesdb.directory.ITimeSeriesDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.ITimeSeriesDirectoryHashKeyVersion;

public interface ITimeSeriesDirectoryHashKey {

    ITimeSeriesDirectory getParent();

    /**
     * The hash key of the data (e.g. instrument/dailyData/segmentKey)
     */
    String getHashKey();

    File getDirectoryHashKeyShared();

    File getDirectoryHashKeyPerNode();

    ITimeSeriesDirectoryHashKeyVersion getDirectoryHashKeyVersion();

}
