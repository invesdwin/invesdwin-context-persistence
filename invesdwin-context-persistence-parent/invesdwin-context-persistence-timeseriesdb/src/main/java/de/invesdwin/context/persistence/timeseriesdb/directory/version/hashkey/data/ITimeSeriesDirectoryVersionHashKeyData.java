package de.invesdwin.context.persistence.timeseriesdb.directory.version.hashkey.data;

import java.io.File;

import de.invesdwin.context.persistence.timeseriesdb.directory.version.hashkey.ITimeSeriesDirectoryVersionHashKey;

public interface ITimeSeriesDirectoryVersionHashKeyData {

    ITimeSeriesDirectoryVersionHashKey getParent();

    /**
     * The type of storage (e.g. segmentStatus, memory)
     */
    String getDataId();

    File getDirectoryVersionHashKeyDataShared();

    File getDirectoryVersionHashKeyDataPerNode();

    void delete();

}
