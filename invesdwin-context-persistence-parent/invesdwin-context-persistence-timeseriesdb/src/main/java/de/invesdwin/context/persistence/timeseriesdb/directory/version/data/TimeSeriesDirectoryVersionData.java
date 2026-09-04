package de.invesdwin.context.persistence.timeseriesdb.directory.version.data;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.timeseriesdb.directory.version.ITimeSeriesDirectoryVersion;
import de.invesdwin.context.system.properties.ICloseableProperties;
import de.invesdwin.context.system.properties.concurrent.multiprocess.AtomicFilesHelper;
import de.invesdwin.context.system.properties.concurrent.multiprocess.TransactionalFileProperties;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Objects;

@Immutable
public class TimeSeriesDirectoryVersionData implements ITimeSeriesDirectoryVersionData {

    private final ITimeSeriesDirectoryVersion parent;
    private final String storageKey;
    private final File directoryVersionDataShared;
    private final File directoryVersionDataPerNode;
    private AtomicFilesHelper propertiesAtomicFilesHelper;

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
        return new TransactionalFileProperties(getPropertiesAtomicFilesHelper());
    }

    private AtomicFilesHelper getPropertiesAtomicFilesHelper() {
        if (propertiesAtomicFilesHelper == null) {
            synchronized (this) {
                if (propertiesAtomicFilesHelper == null) {
                    propertiesAtomicFilesHelper = new AtomicFilesHelper(
                            TransactionalFileProperties.newDefaultFolder(directoryVersionDataShared));
                }
            }
        }
        return propertiesAtomicFilesHelper;
    }

}
