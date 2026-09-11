package de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.lease;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.ITimeSeriesDirectoryHashKey;
import de.invesdwin.util.error.RuntimeIOException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.streams.closeable.ISafeCloseable;

@ThreadSafe
public final class TimeSeriesDirectoryHashKeyVersionLease implements ISafeCloseable {

    public static final TimeSeriesDirectoryHashKeyVersionLease[] EMPTY_ARRAY = new TimeSeriesDirectoryHashKeyVersionLease[0];

    private final String registryKey;
    private final int version;
    private final File directoryHashKeyVersionShared;
    private final File directoryHashKeyVersionPerNode;
    private final AtomicInteger refCount = new AtomicInteger(0);
    private final ITimeSeriesDirectoryHashKey parent;

    TimeSeriesDirectoryHashKeyVersionLease(final String registryKey, final ITimeSeriesDirectoryHashKey parent,
            final int version) {
        this.registryKey = registryKey;
        this.parent = parent;
        this.version = version;
        this.directoryHashKeyVersionShared = new File(parent.getDirectoryHashKeyShared(), String.valueOf(version));
        this.directoryHashKeyVersionPerNode = new File(parent.getDirectoryHashKeyPerNode(), String.valueOf(version));

        try {
            Files.forceMkdir(directoryHashKeyVersionShared);
            Files.forceMkdir(directoryHashKeyVersionPerNode);
        } catch (final IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public String getRegistryKey() {
        return registryKey;
    }

    public ITimeSeriesDirectoryHashKey getParent() {
        return parent;
    }

    public void retain() {
        refCount.incrementAndGet();
    }

    public int getVersion() {
        return version;
    }

    public File getDirectoryHashKeyVersionShared() {
        return directoryHashKeyVersionShared;
    }

    public File getDirectoryHashKeyVersionPerNode() {
        return directoryHashKeyVersionPerNode;
    }

    @Override
    public void close() {
        if (refCount.decrementAndGet() <= 0) {
            TimeSeriesDirectoryHashKeyVersionLeaseRegistry.remove(this);
        }
    }

    public void delete() {
        Files.deleteNative(directoryHashKeyVersionShared);
        if (!Objects.equals(directoryHashKeyVersionShared, directoryHashKeyVersionPerNode)) {
            Files.deleteNative(directoryHashKeyVersionPerNode);
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(directoryHashKeyVersionShared.getAbsolutePath()).toString();
    }
}