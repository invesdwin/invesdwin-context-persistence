package de.invesdwin.context.persistence.timeseriesdb.directory.version.lock;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.directory.ITimeSeriesDirectory;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.concurrent.Executors;

@ThreadSafe
public final class TimeSeriesDirectoryVersionLeaseRegistry {

    private static final Map<String, TimeSeriesDirectoryVersionLease> REGISTRY = ILockCollectionFactory
            .getInstance(true)
            .newConcurrentMap();

    // Use a lock object to synchronize creation and destruction of the executor
    private static final Object EXECUTOR_LOCK = new Object();
    private static ScheduledExecutorService heartbeatExecutor;

    private TimeSeriesDirectoryVersionLeaseRegistry() {}

    public static TimeSeriesDirectoryVersionLease getOrCreate(final ITimeSeriesDirectory parent, final String version) {
        final String registryKey = parent.getDirectoryShared().getAbsolutePath() + "/" + version;
        final TimeSeriesDirectoryVersionLease lease = REGISTRY.compute(registryKey, (key, existingLease) -> {
            TimeSeriesDirectoryVersionLease current = existingLease;
            if (current == null) {
                current = new TimeSeriesDirectoryVersionLease(parent, version);
            }
            current.retain();
            return current;
        });

        startHeartbeatExecutorIfNeeded();
        return lease;
    }

    static void remove(final String version, final TimeSeriesDirectoryVersionLease lease) {
        final String registryKey = lease.getDirectoryVersionShared().getParent() + "/" + version;
        REGISTRY.remove(registryKey, lease);

        stopHeartbeatExecutorIfNeeded();
    }

    private static void startHeartbeatExecutorIfNeeded() {
        synchronized (EXECUTOR_LOCK) {
            if (heartbeatExecutor == null || heartbeatExecutor.isShutdown()) {
                heartbeatExecutor = Executors
                        .newScheduledThreadPool(TimeSeriesDirectoryVersionLeaseRegistry.class.getSimpleName(), 1);
                heartbeatExecutor.scheduleAtFixedRate(TimeSeriesDirectoryVersionLeaseRegistry::updateHeartbeats, 1, 1,
                        TimeUnit.MINUTES);
            }
        }
    }

    private static void stopHeartbeatExecutorIfNeeded() {
        synchronized (EXECUTOR_LOCK) {
            // Only shut down if the registry is empty and the executor is actively running
            if (REGISTRY.isEmpty() && heartbeatExecutor != null && !heartbeatExecutor.isShutdown()) {
                heartbeatExecutor.shutdown();
                heartbeatExecutor = null;
            }
        }
    }

    private static void updateHeartbeats() {
        final Iterator<Entry<String, TimeSeriesDirectoryVersionLease>> iterator = REGISTRY.entrySet().iterator();
        while (iterator.hasNext()) {
            final Entry<String, TimeSeriesDirectoryVersionLease> entry = iterator.next();
            final TimeSeriesDirectoryVersionLease lease = entry.getValue();
            if (lease != null) {
                lease.touchHeartbeat();
            }
        }

        // Safety check just in case the registry emptied out during an active heartbeat sweep
        stopHeartbeatExecutorIfNeeded();
    }
}