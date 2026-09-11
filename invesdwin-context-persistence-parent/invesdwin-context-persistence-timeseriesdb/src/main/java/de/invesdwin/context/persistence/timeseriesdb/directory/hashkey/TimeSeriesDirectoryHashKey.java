package de.invesdwin.context.persistence.timeseriesdb.directory.hashkey;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.directory.ITimeSeriesDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.ITimeSeriesDirectoryHashKeyVersion;
import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.TimeSeriesDirectoryHashKeyVersion;
import de.invesdwin.util.lang.Files;

@ThreadSafe
public class TimeSeriesDirectoryHashKey implements ITimeSeriesDirectoryHashKey {

    private final ITimeSeriesDirectory parent;
    private final String hashKey;
    private final TimeSeriesDirectoryHashKeyVersion directoryHashKeyVersion;
    private File directoryHashKeyShared;
    private File directoryHashKeyPerNode;

    public TimeSeriesDirectoryHashKey(final ITimeSeriesDirectory parent, final String hashKey) {
        this.parent = parent;
        this.hashKey = hashKey;
        this.directoryHashKeyVersion = new TimeSeriesDirectoryHashKeyVersion(this);
    }

    @Override
    public ITimeSeriesDirectory getParent() {
        return parent;
    }

    @Override
    public String getHashKey() {
        return hashKey;
    }

    @Override
    public File getDirectoryHashKeyShared() {
        if (directoryHashKeyShared == null) {
            synchronized (this) {
                if (directoryHashKeyShared == null) {
                    directoryHashKeyShared = new File(parent.getDirectoryShared(), hashKey);
                    try {
                        Files.forceMkdir(directoryHashKeyShared);
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return directoryHashKeyShared;
    }

    @Override
    public File getDirectoryHashKeyPerNode() {
        if (directoryHashKeyPerNode == null) {
            synchronized (this) {
                if (directoryHashKeyPerNode == null) {
                    directoryHashKeyPerNode = new File(parent.getDirectoryPerNode(), hashKey);
                    try {
                        Files.forceMkdir(directoryHashKeyPerNode);
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return directoryHashKeyShared;
    }

    @Override
    public ITimeSeriesDirectoryHashKeyVersion getDirectoryHashKeyVersion() {
        return directoryHashKeyVersion;
    }

}
