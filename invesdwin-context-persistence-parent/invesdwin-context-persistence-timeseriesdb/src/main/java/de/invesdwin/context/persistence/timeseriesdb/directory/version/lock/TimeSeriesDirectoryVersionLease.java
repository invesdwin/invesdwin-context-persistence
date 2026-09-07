package de.invesdwin.context.persistence.timeseriesdb.directory.version.lock;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.persistence.timeseriesdb.directory.ITimeSeriesDirectory;
import de.invesdwin.instrument.DynamicInstrumentationProperties;
import de.invesdwin.util.error.RuntimeIOException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.string.Charsets;
import de.invesdwin.util.streams.closeable.ISafeCloseable;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.millis.FDateMillis;

@ThreadSafe
public final class TimeSeriesDirectoryVersionLease implements ISafeCloseable {
    private final int version;
    private final File directoryVersionShared;
    private final File directoryVersionPerNode;
    private final File heartbeatFile;
    private final AtomicInteger refCount = new AtomicInteger(0);

    TimeSeriesDirectoryVersionLease(final ITimeSeriesDirectory parent, final int version) {
        this.version = version;
        this.directoryVersionShared = new File(parent.getDirectoryShared(), String.valueOf(version));
        this.directoryVersionPerNode = new File(parent.getDirectoryPerNode(), String.valueOf(version));

        try {
            Files.forceMkdir(directoryVersionShared);
            Files.forceMkdir(directoryVersionPerNode);
        } catch (final IOException e) {
            throw new RuntimeIOException(e);
        }

        this.heartbeatFile = new File(this.directoryVersionShared,
                "lease_" + UUID.randomUUID().toString() + ".heartbeat");
        try {
            final StringBuilder heartbeatContent = new StringBuilder();
            heartbeatContent.append("Version: ").append(version).append("\n");
            heartbeatContent.append("Hostname: ").append(IntegrationProperties.HOSTNAME).append("\n");
            heartbeatContent.append("ProcessId: ").append(DynamicInstrumentationProperties.getProcessId()).append("\n");
            heartbeatContent.append("ProcessName: ")
                    .append(DynamicInstrumentationProperties.getProcessName())
                    .append("\n");
            heartbeatContent.append("Created: ").append(FDate.now());
            Files.writeStringToFile(heartbeatFile, heartbeatContent.toString(), Charsets.defaultCharset());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        touchHeartbeat();
    }

    public void retain() {
        refCount.incrementAndGet();
    }

    public int getVersion() {
        return version;
    }

    public File getDirectoryVersionShared() {
        return directoryVersionShared;
    }

    public File getDirectoryVersionPerNode() {
        return directoryVersionPerNode;
    }

    void touchHeartbeat() {
        try {
            if (!heartbeatFile.exists()) {
                heartbeatFile.createNewFile();
            } else {
                heartbeatFile.setLastModified(FDateMillis.nowMillis());
            }
        } catch (final IOException e) {
            throw new RuntimeException("Failed to update heartbeat lock file: " + heartbeatFile.getAbsolutePath(), e);
        }
    }

    @Override
    public void close() {
        if (refCount.decrementAndGet() <= 0) {
            TimeSeriesDirectoryVersionLeaseRegistry.remove(version, this);
            if (heartbeatFile.exists()) {
                heartbeatFile.delete();
            }
        }
    }

    public void delete() {
        Files.deleteNative(directoryVersionShared);
        if (!Objects.equals(directoryVersionShared, directoryVersionPerNode)) {
            Files.deleteNative(directoryVersionPerNode);
        }
    }
}