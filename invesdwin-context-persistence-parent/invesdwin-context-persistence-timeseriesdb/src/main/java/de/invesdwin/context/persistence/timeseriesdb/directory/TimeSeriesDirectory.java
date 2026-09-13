package de.invesdwin.context.persistence.timeseriesdb.directory;

import java.io.File;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Objects;

@Immutable
public class TimeSeriesDirectory implements ITimeSeriesDirectory {

    public static final String HEARTBEATS_FOLDER_NAME = "heartbeats";

    private final ITimeSeriesBaseDirectory parent;
    private final String storageName;
    private final File directoryShared;
    private final File directoryPerNode;
    private final File heartbeatsDirectory;

    public TimeSeriesDirectory(final ITimeSeriesBaseDirectory parent, final String storageName) {
        this.parent = parent;
        this.storageName = storageName;
        this.directoryShared = new File(parent.getBaseDirectoryShared(), storageName);
        this.directoryPerNode = new File(parent.getBaseDirectoryPerNode(), storageName);
        this.heartbeatsDirectory = new File(directoryShared, HEARTBEATS_FOLDER_NAME);
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
    public File getHeartbeatsDirectory() {
        return heartbeatsDirectory;
    }

    @Override
    public void deleteCorruptedStorage() {
        Files.deleteNative(directoryShared);
        if (!Objects.equals(directoryShared, directoryPerNode)) {
            Files.deleteNative(directoryPerNode);
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(directoryShared.getAbsolutePath()).toString();
    }

}