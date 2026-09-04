package de.invesdwin.context.persistence.timeseriesdb.directory.base;

import java.io.File;

public interface ITimeSeriesBaseDirectory {

    File getBaseDirectoryShared();

    File getBaseDirectoryPerNode();

    void delete();

}
