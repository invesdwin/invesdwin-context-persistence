package de.invesdwin.context.persistence.timeseriesdb.directory.version.hashkey.data;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.timeseriesdb.directory.version.hashkey.ITimeSeriesDirectoryVersionHashKey;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Objects;

@Immutable
public class TimeSeriesDirectoryVersionHashKeyData implements ITimeSeriesDirectoryVersionHashKeyData {

    private final ITimeSeriesDirectoryVersionHashKey parent;
    private final String dataId;
    private File directoryVersionHashKeyDataShared;
    private File directoryVersionHashKeyDataPerNode;

    public TimeSeriesDirectoryVersionHashKeyData(final ITimeSeriesDirectoryVersionHashKey parent, final String dataId) {
        this.parent = parent;
        this.dataId = dataId;
    }

    @Override
    public ITimeSeriesDirectoryVersionHashKey getParent() {
        return parent;
    }

    @Override
    public String getDataId() {
        return dataId;
    }

    @Override
    public File getDirectoryVersionHashKeyDataShared() {
        if (directoryVersionHashKeyDataShared == null) {
            synchronized (this) {
                if (directoryVersionHashKeyDataShared == null) {
                    directoryVersionHashKeyDataShared = new File(parent.getDirectoryVersionHashKeyShared(), dataId);
                    try {
                        Files.forceMkdir(directoryVersionHashKeyDataShared);
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return directoryVersionHashKeyDataShared;
    }

    @Override
    public File getDirectoryVersionHashKeyDataPerNode() {
        if (directoryVersionHashKeyDataPerNode == null) {
            synchronized (this) {
                if (directoryVersionHashKeyDataPerNode == null) {
                    directoryVersionHashKeyDataPerNode = new File(parent.getDirectoryVersionHashKeyPerNode(), dataId);
                    try {
                        Files.forceMkdir(directoryVersionHashKeyDataPerNode);
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return directoryVersionHashKeyDataPerNode;
    }

    @Override
    public void delete() {
        //System.out.println("TODO: rework this");
        Files.deleteNative(directoryVersionHashKeyDataShared);
        if (!Objects.equals(directoryVersionHashKeyDataShared, directoryVersionHashKeyDataPerNode)) {
            Files.deleteNative(directoryVersionHashKeyDataPerNode);
        }
    }

}
