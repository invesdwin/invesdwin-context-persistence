package de.invesdwin.context.persistence.timeseriesdb.directory.version.data;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannelPath;
import de.invesdwin.context.integration.filechannel.nio.atomic.properties.TransactionalFileProperties;
import de.invesdwin.context.persistence.timeseriesdb.directory.version.ITimeSeriesDirectoryVersion;
import de.invesdwin.context.system.properties.ICloseableProperties;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Objects;

@Immutable
public class TimeSeriesDirectoryVersionData implements ITimeSeriesDirectoryVersionData {

    private final ITimeSeriesDirectoryVersion parent;
    private final String storageKey;
    private final File directoryVersionDataShared;
    private final File directoryVersionDataPerNode;
    private AtomicNioFileChannelPath propertiesPath;

    public TimeSeriesDirectoryVersionData(final ITimeSeriesDirectoryVersion parent, final String storageKey) {
        this.parent = parent;
        this.storageKey = storageKey;
        this.directoryVersionDataShared = new File(parent.getDirectoryVersionShared(), storageKey);
        this.directoryVersionDataPerNode = new File(parent.getDirectoryVersionPerNode(), storageKey);
        try {
            Files.forceMkdir(directoryVersionDataShared);
            Files.forceMkdir(directoryVersionDataPerNode);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ITimeSeriesDirectoryVersion getParent() {
        return parent;
    }

    @Override
    public String getStorageKey() {
        return storageKey;
    }

    @Override
    public File getDirectoryVersionDataShared() {
        return directoryVersionDataShared;
    }

    @Override
    public File getDirectoryVersionDataPerNode() {
        return directoryVersionDataPerNode;
    }

    @Override
    public void delete() {
        Files.deleteNative(directoryVersionDataShared);
        if (!Objects.equals(directoryVersionDataShared, directoryVersionDataPerNode)) {
            Files.deleteNative(directoryVersionDataPerNode);
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
                            TransactionalFileProperties.newDefaultFolder(directoryVersionDataShared).toURI());
                }
            }
        }
        return propertiesPath;
    }

}
