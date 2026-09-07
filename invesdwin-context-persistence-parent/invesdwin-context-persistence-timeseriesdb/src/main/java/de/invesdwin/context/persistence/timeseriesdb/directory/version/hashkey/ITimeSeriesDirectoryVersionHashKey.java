package de.invesdwin.context.persistence.timeseriesdb.directory.version.hashkey;

import java.io.File;

import de.invesdwin.context.persistence.timeseriesdb.directory.version.ITimeSeriesDirectoryVersion;
import de.invesdwin.context.system.properties.ICloseableProperties;

public interface ITimeSeriesDirectoryVersionHashKey {

    ITimeSeriesDirectoryVersion getParent();

    /**
     * The hash key of the data (e.g. instrument/dailyData/segmentKey)
     */
    String getHashKey();

    File getDirectoryVersionHashKeyShared();

    File getDirectoryVersionHashKeyPerNode();

    void delete();

    ICloseableProperties getProperties();

}
