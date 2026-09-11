package de.invesdwin.context.persistence.timeseriesdb.directory.base;

import java.io.File;

public interface ITimeSeriesBaseDirectory {

    File getBaseDirectoryShared();

    File getBaseDirectoryPerNode();

    /**
     * WARNING: use this only for temporary tables that are not shared between processes
     */
    void delete();

}
