package de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.data;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.ITimeSeriesDirectoryHashKeyVersion;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Objects;

@Immutable
public class TimeSeriesDirectoryHashKeyVersionData implements ITimeSeriesDirectoryHashKeyVersionData {

    private final ITimeSeriesDirectoryHashKeyVersion parent;
    private final String dataId;
    private File directoryHashKeyVersionDataShared;
    private File directoryHashKeyVersionDataPerNode;

    public TimeSeriesDirectoryHashKeyVersionData(final ITimeSeriesDirectoryHashKeyVersion parent, final String dataId) {
        this.parent = parent;
        this.dataId = dataId;
    }

    @Override
    public ITimeSeriesDirectoryHashKeyVersion getParent() {
        return parent;
    }

    @Override
    public String getDataId() {
        return dataId;
    }

    @Override
    public File getDirectoryHashKeyVersionDataShared() {
        if (directoryHashKeyVersionDataShared == null) {
            synchronized (this) {
                if (directoryHashKeyVersionDataShared == null) {
                    directoryHashKeyVersionDataShared = new File(parent.getDirectoryHashKeyVersionShared(), dataId);
                    try {
                        Files.forceMkdir(directoryHashKeyVersionDataShared);
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return directoryHashKeyVersionDataShared;
    }

    @Override
    public File getDirectoryHashKeyVersionDataPerNode() {
        if (directoryHashKeyVersionDataPerNode == null) {
            synchronized (this) {
                if (directoryHashKeyVersionDataPerNode == null) {
                    directoryHashKeyVersionDataPerNode = new File(parent.getDirectoryHashKeyVersionPerNode(), dataId);
                    try {
                        Files.forceMkdir(directoryHashKeyVersionDataPerNode);
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return directoryHashKeyVersionDataPerNode;
    }

    @Override
    public void delete() {
        //System.out.println("TODO: rework this");
        Files.deleteNative(directoryHashKeyVersionDataShared);
        if (!Objects.equals(directoryHashKeyVersionDataShared, directoryHashKeyVersionDataPerNode)) {
            Files.deleteNative(directoryHashKeyVersionDataPerNode);
        }
    }

}
