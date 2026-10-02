package de.invesdwin.context.persistence.timeseriesdb.directory;

import java.io.File;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.filechannel.nio.atomic.properties.AtomicFilesProperties;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.system.properties.IProperties;
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
    private AtomicFilesProperties storagePropertiesShared;
    private AtomicFilesProperties storagePropertiesPerNode;

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
        storagePropertiesShared = null;
        storagePropertiesPerNode = null;
    }

    @Override
    public IProperties getStoragePropertiesShared() {
        if (storagePropertiesShared == null) {
            synchronized (this) {
                if (storagePropertiesShared == null) {
                    this.storagePropertiesShared = new AtomicFilesProperties(
                            new File(directoryShared, "storageProperties"));
                }
            }
        }
        return storagePropertiesShared;
    }

    @Override
    public IProperties getStoragePropertiesPerNode() {
        if (storagePropertiesPerNode == null) {
            synchronized (this) {
                if (storagePropertiesPerNode == null) {
                    this.storagePropertiesPerNode = new AtomicFilesProperties(
                            new File(directoryPerNode, "storageProperties"));
                }
            }
        }
        return storagePropertiesPerNode;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(directoryShared.getAbsolutePath()).toString();
    }

}