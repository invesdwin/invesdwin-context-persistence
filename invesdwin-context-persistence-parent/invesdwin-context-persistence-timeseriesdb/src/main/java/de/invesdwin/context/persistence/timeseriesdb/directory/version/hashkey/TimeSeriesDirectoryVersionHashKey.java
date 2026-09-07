package de.invesdwin.context.persistence.timeseriesdb.directory.version.hashkey;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannelPath;
import de.invesdwin.context.integration.filechannel.nio.atomic.properties.TransactionalFileProperties;
import de.invesdwin.context.persistence.timeseriesdb.directory.version.ITimeSeriesDirectoryVersion;
import de.invesdwin.context.system.properties.ICloseableProperties;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Objects;

@ThreadSafe
public class TimeSeriesDirectoryVersionHashKey implements ITimeSeriesDirectoryVersionHashKey {

    private final ITimeSeriesDirectoryVersion parent;
    private final String hashKey;
    private File directoryVersionHashKeyShared;
    private File directoryVersionHashKeyPerNode;
    private AtomicNioFileChannelPath propertiesPath;

    public TimeSeriesDirectoryVersionHashKey(final ITimeSeriesDirectoryVersion parent, final String hashKey) {
        this.parent = parent;
        this.hashKey = hashKey;
    }

    @Override
    public ITimeSeriesDirectoryVersion getParent() {
        return parent;
    }

    @Override
    public String getHashKey() {
        return hashKey;
    }

    @Override
    public File getDirectoryVersionHashKeyShared() {
        if (directoryVersionHashKeyShared == null) {
            synchronized (this) {
                if (directoryVersionHashKeyShared == null) {
                    directoryVersionHashKeyShared = new File(parent.getDirectoryVersionShared(), hashKey);
                    try {
                        Files.forceMkdir(directoryVersionHashKeyShared);
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return directoryVersionHashKeyShared;
    }

    @Override
    public File getDirectoryVersionHashKeyPerNode() {
        if (directoryVersionHashKeyPerNode == null) {
            synchronized (this) {
                if (directoryVersionHashKeyPerNode == null) {
                    directoryVersionHashKeyPerNode = new File(parent.getDirectoryVersionPerNode(), hashKey);
                    try {
                        Files.forceMkdir(directoryVersionHashKeyPerNode);
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return directoryVersionHashKeyShared;
    }

    @Override
    public void delete() {
        //System.out.println("TODO: rework this");
        Files.deleteNative(directoryVersionHashKeyShared);
        if (!Objects.equals(directoryVersionHashKeyShared, directoryVersionHashKeyPerNode)) {
            Files.deleteNative(directoryVersionHashKeyPerNode);
        }
    }

    @Override
    public ICloseableProperties getProperties() {
        //System.out.println(
        //        "TODO: create a wrapper that moved to the new directory on close if delete happened inbetween? also maybe add a flush operation before switching to a new directory?");
        return new TransactionalFileProperties(getPropertiesPath());
    }

    private AtomicNioFileChannelPath getPropertiesPath() {
        if (propertiesPath == null) {
            synchronized (this) {
                if (propertiesPath == null) {
                    propertiesPath = new AtomicNioFileChannelPath(
                            TransactionalFileProperties.newDefaultDirectory(directoryVersionHashKeyShared).toURI());
                }
            }
        }
        return propertiesPath;
    }

}
