package de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version;

import java.io.File;

import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.ITimeSeriesDirectoryHashKey;
import de.invesdwin.context.system.properties.ICloseableProperties;

public interface ITimeSeriesDirectoryHashKeyVersion {

    ITimeSeriesDirectoryHashKey getParent();

    int getVersion();

    File getDirectoryHashKeyVersionShared();

    File getDirectoryHashKeyVersionPerNode();

    void delete();

    ICloseableProperties getProperties();

}
