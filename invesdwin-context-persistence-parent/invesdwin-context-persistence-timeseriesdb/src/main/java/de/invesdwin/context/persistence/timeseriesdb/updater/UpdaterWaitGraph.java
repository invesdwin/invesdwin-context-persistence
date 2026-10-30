package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.collections.factory.ILockCollectionFactory;

/**
 * Per-JVM wait-for graph for the time series updater chain; implements deterministic deadlock avoidance: before any
 * thread adds a blocking wait edge (parking on an update lock, joining an update future, or patiently waiting for a
 * heartbeat pin owned inside this JVM) the graph is walked from the target key - if the walk leads back to the waiting
 * thread (it owns a pin, an update lock, an in-flight update task, or a wait of its own that the chain passes
 * through), the wait would close a cycle and the attempt is aborted retryably instead of hanging forever (the
 * permanent parallel-precalculated livelock this class removes).
 *
 * Cross-process waits are treated as cycle-free leaves: another process can never depend on a resource of this JVM
 * here (their heartbeats either progress or time out, which the steal path handles).
 */
@ThreadSafe
public final class UpdaterWaitGraph {

    private static final Map<Thread, Set<String>> WAIT_ON = ILockCollectionFactory.getInstance(true)
            .newConcurrentMap();
    private static final Map<String, Thread> PIN_OWNER = ILockCollectionFactory.getInstance(true).newConcurrentMap();
    private static final Map<String, Thread> LOCK_OWNER = ILockCollectionFactory.getInstance(true).newConcurrentMap();
    private static final Map<String, Thread> TASK_RUNNING = ILockCollectionFactory.getInstance(true).newConcurrentMap();

    private UpdaterWaitGraph() {}

    public static String newKey(final String tableName, final String keyStr) {
        return String.valueOf(tableName) + "|" + String.valueOf(keyStr);
    }

    public static void beginWait(final String key) {
        WAIT_ON.computeIfAbsent(Thread.currentThread(), t -> ILockCollectionFactory.getInstance(true)
                .newConcurrentSet()).add(key);
    }

    public static void endWait(final String key) {
        final Set<String> waits = WAIT_ON.get(Thread.currentThread());
        if (waits != null) {
            waits.remove(key);
            if (waits.isEmpty()) {
                WAIT_ON.remove(Thread.currentThread(), waits);
            }
        }
    }

    public static void lockAcquired(final String key) {
        LOCK_OWNER.put(key, Thread.currentThread());
    }

    public static void lockReleased(final String key) {
        LOCK_OWNER.remove(key, Thread.currentThread());
    }

    public static void taskStarted(final String key) {
        TASK_RUNNING.put(key, Thread.currentThread());
    }

    public static void taskFinished(final String key) {
        TASK_RUNNING.remove(key, Thread.currentThread());
    }

    public static void pinAcquired(final String key) {
        PIN_OWNER.put(key, Thread.currentThread());
    }

    /**
     * Releases a pin; may be called from another thread (warning-finalizer path), hence the explicit owner.
     */
    public static void pinReleased(final String key, final Thread owner) {
        if (owner != null) {
            PIN_OWNER.remove(key, owner);
        }
    }

    /**
     * True when the current thread blocking on the given key would close a wait-for cycle within this JVM.
     */
    public static boolean wouldCloseCycle(final String key) {
        final Thread self = Thread.currentThread();
        //Thread has no value-equality, so plain hash sets are exactly identity sets here
        final Set<String> visitedKeys = ILockCollectionFactory.getInstance(false).newSet();
        final Set<Thread> visitedThreads = ILockCollectionFactory.getInstance(false).newSet();
        return walkKey(key, self, visitedKeys, visitedThreads, 0);
    }

    private static boolean walkKey(final String key, final Thread self, final Set<String> visitedKeys,
            final Set<Thread> visitedThreads, final int depth) {
        if (depth > 64 || !visitedKeys.add(key)) {
            return false;
        }
        return walkThread(PIN_OWNER.get(key), self, visitedKeys, visitedThreads, depth)
                || walkThread(LOCK_OWNER.get(key), self, visitedKeys, visitedThreads, depth)
                || walkThread(TASK_RUNNING.get(key), self, visitedKeys, visitedThreads, depth);
    }

    private static boolean walkThread(final Thread owner, final Thread self, final Set<String> visitedKeys,
            final Set<Thread> visitedThreads, final int depth) {
        if (owner == null) {
            return false;
        }
        if (owner == self) {
            return true;
        }
        if (!visitedThreads.add(owner)) {
            return false;
        }
        final Set<String> waits = WAIT_ON.get(owner);
        if (waits == null || waits.isEmpty()) {
            return false;
        }
        for (final String otherKey : ILockCollectionFactory.getInstance(false).newArrayList(waits)) {
            if (walkKey(otherKey, self, visitedKeys, visitedThreads, depth + 1)) {
                return true;
            }
        }
        return false;
    }

    public static String describeCycle(final String key) {
        final StringBuilder sb = new StringBuilder("wait-for chain for [").append(key).append("]:");
        final Thread self = Thread.currentThread();
        final Set<String> visitedKeys = ILockCollectionFactory.getInstance(false).newSet();
        final Set<Thread> visitedThreads = ILockCollectionFactory.getInstance(false).newSet();
        describeKey(key, self, visitedKeys, visitedThreads, sb);
        return sb.toString();
    }

    private static void describeKey(final String key, final Thread self, final Set<String> visitedKeys,
            final Set<Thread> visitedThreads, final StringBuilder sb) {
        if (!visitedKeys.add(key)) {
            return;
        }
        describeThread("pin", PIN_OWNER.get(key), self, visitedKeys, visitedThreads, sb);
        describeThread("lock", LOCK_OWNER.get(key), self, visitedKeys, visitedThreads, sb);
        describeThread("task", TASK_RUNNING.get(key), self, visitedKeys, visitedThreads, sb);
    }

    private static void describeThread(final String role, final Thread owner, final Thread self,
            final Set<String> visitedKeys, final Set<Thread> visitedThreads, final StringBuilder sb) {
        if (owner == null) {
            return;
        }
        sb.append(" ").append(role).append("=[").append(owner.getName()).append("]");
        if (owner == self || !visitedThreads.add(owner)) {
            return;
        }
        final Set<String> waits = WAIT_ON.get(owner);
        if (waits != null) {
            for (final String otherKey : ILockCollectionFactory.getInstance(false).newArrayList(waits)) {
                describeKey(otherKey, self, visitedKeys, visitedThreads, sb);
            }
        }
    }

}
