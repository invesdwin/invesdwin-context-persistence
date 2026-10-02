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
    private volatile File directoryHashKeyVersionDataShared;
    private volatile File directoryHashKeyVersionDataPerNode;
    private volatile int version;

    public TimeSeriesDirectoryHashKeyVersionData(final ITimeSeriesDirectoryHashKeyVersion parent, final String dataId) {
        this.parent = parent;
        this.dataId = dataId;
        this.version = parent.getVersion();
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
        maybeReset();
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
        maybeReset();
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

    private void maybeReset() {
        if (version != parent.getVersion()) {
            synchronized (this) {
                if (version != parent.getVersion()) {
                    directoryHashKeyVersionDataShared = null;
                    directoryHashKeyVersionDataPerNode = null;
                    version = parent.getVersion();
                }
            }
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("dataId", dataId).with(parent).toString();
    }

}
