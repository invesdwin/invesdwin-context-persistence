package de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.data;

import java.io.File;

import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.ITimeSeriesDirectoryHashKeyVersion;

public interface ITimeSeriesDirectoryHashKeyVersionData {

    ITimeSeriesDirectoryHashKeyVersion getParent();

    /**
     * The type of storage (e.g. segmentStatus, memory)
     */
    String getDataId();

    File getDirectoryHashKeyVersionDataShared();

    File getDirectoryHashKeyVersionDataPerNode();

    void delete();

}
