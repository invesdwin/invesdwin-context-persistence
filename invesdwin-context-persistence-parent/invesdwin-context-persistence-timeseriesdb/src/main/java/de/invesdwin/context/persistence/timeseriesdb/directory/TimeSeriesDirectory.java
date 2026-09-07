package de.invesdwin.context.persistence.timeseriesdb.directory;

import java.io.File;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;

@Immutable
public class TimeSeriesDirectory implements ITimeSeriesDirectory {

    private final ITimeSeriesBaseDirectory parent;
    private final String storageName;
    private final File directoryShared;
    private final File directoryPerNode;
    private final File heartbeatDirectory;

    public TimeSeriesDirectory(final ITimeSeriesBaseDirectory parent, final String storageName) {
        this.parent = parent;
        this.storageName = storageName;
        this.directoryShared = new File(parent.getBaseDirectoryShared(), storageName);
        this.directoryPerNode = new File(parent.getBaseDirectoryPerNode(), storageName);
        this.heartbeatDirectory = new File(directoryShared, "heartbeat");
    }

    @Override
    public ITimeSeriesBaseDirectory getParent() {
        return parent;
    }

    @Override
    public String getStorageName() {
        return storageName;
    }

    @Override
    public File getDirectoryShared() {
        return directoryShared;
    }

    @Override
    public File getDirectoryPerNode() {
        return directoryPerNode;
    }

    @Override
    public File getHeartbeatDirectory() {
        return heartbeatDirectory;
    }

    @Override
    public void delete() {
        //        System.out.println(
        //                "TODO: create a new version and add a cleanup procedure, though should also be isolated per key?");
        //maybe atomic rename the folder to _deleted and delete async if this process succeeded in delete? or should we add another version layer?
        //or should we just reset/delete the perNode data? though I guess we need to handle data format changes with a complete reset?
    }

}
