package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import io.netty.util.concurrent.FastThreadLocal;

/**
 * Per-thread epoch scopes: each running update attempt (epoch) knows its own epoch id and the chain of ancestor epoch
 * ids that led to it; a demand tree that adopts a sibling's staged epoch snapshot records the consumed
 * '&lt;key&gt;#&lt;epochId&gt;' entries into the innermost open scope, from where the pass's progress writer serialises
 * them into the progress marker (see {@link TimeSeriesUpdaterProgress}) for the later commit-time re-check.
 *
 * <p>
 * Isolation is a properly balanced FastThreadLocal (try-with-resources restores the previous scope on close, so pool
 * thread reuse cannot leak state). Cross-thread coverage is EXPLICIT, never inferred from thread names: a submitting
 * context {@link #capture()}s its visible epoch chain and the task body {@link #install(List)}s it - exactly the
 * pattern already proven for {@link de.invesdwin.context.integration.retry.RetryScopes} at the lazy data updater submit
 * sites (ALazyDataUpdater.maybeUpdate). No maps, no thread names.
 */
@ThreadSafe
public final class EpochScopes {

    private static final FastThreadLocal<EpochScope> SCOPE = new FastThreadLocal<EpochScope>() {

        @Override
        protected EpochScope initialValue() {
            return null;
        }
    };

    private EpochScopes() {}

    @NotThreadSafe
    public static final class EpochScope implements AutoCloseable {

        private final EpochScope prev;
        private final String ownEpochId;
        private final List<String> ancestorEpochIds;
        private final Set<String> consumedEntries;
        private boolean closed;

        private EpochScope(final EpochScope prev, final String ownEpochId, final List<String> ancestorEpochIds) {
            this.prev = prev;
            this.ownEpochId = ownEpochId;
            this.ancestorEpochIds = ancestorEpochIds;
            this.consumedEntries = ILockCollectionFactory.getInstance(false).newLinkedSet();
        }

        /**
         * Restores the previous scope (balanced close; closing twice or out of order is refused loudly).
         */
        @Override
        public void close() {
            if (closed) {
                throw new IllegalStateException("Epoch scope closed twice: " + this);
            }
            //only the innermost open scope may be closed (balanced like RetryScopes registrations)
            if (SCOPE.get() != this) {
                throw new IllegalStateException("Epoch scope closed out of order: " + this);
            }
            closed = true;
            if (prev == null) {
                SCOPE.remove();
            } else {
                SCOPE.set(prev);
            }
        }

        public String getOwnEpochId() {
            return ownEpochId;
        }

        public List<String> getAncestorEpochIds() {
            return ancestorEpochIds;
        }

        public List<String> getConsumedEntries() {
            return new ArrayList<>(consumedEntries);
        }

        /**
         * True when the given epoch id is this scope's own epoch or one of its captured ancestors, i.e. the staged
         * epoch is part of this thread's demand tree and adopting it cannot read below the tree's own invariants.
         */
        public boolean isEpochVisible(final String epochId) {
            if (epochId == null) {
                return false;
            }
            if (epochId.equals(ownEpochId)) {
                return true;
            }
            for (final String ancestor : ancestorEpochIds) {
                if (epochId.equals(ancestor)) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Opens a new epoch scope for a running update attempt: the current thread's visible epoch chain becomes the
     * ancestor chain of the new scope (an update attempt nested inside another demand sees the epochs around it).
     * Close via try-with-resources at the end of the attempt.
     */
    public static EpochScope openEpoch(final String ownEpochId) {
        final EpochScope prev = SCOPE.get();
        final EpochScope scope = new EpochScope(prev, ownEpochId, capture());
        SCOPE.set(scope);
        return scope;
    }

    /**
     * Installs a captured epoch chain on the current thread for the duration of a submitted task body (see class
     * javadoc); the previous scope is restored on close.
     */
    public static EpochScope install(final List<String> capturedAncestorEpochIds) {
        final EpochScope prev = SCOPE.get();
        final EpochScope scope = new EpochScope(prev, null, new ArrayList<>(capturedAncestorEpochIds));
        SCOPE.set(scope);
        return scope;
    }

    /**
     * Snapshot of the epoch ids visible to the current thread (innermost own id first, then the captured ancestor
     * chains); empty when no epoch scope is open.
     */
    public static List<String> capture() {
        final List<String> ids = new ArrayList<>();
        EpochScope scope = SCOPE.get();
        while (scope != null) {
            if (scope.ownEpochId != null) {
                ids.add(scope.ownEpochId);
            }
            ids.addAll(scope.ancestorEpochIds);
            scope = scope.prev;
        }
        return ids;
    }

    /**
     * Records a consumed epoch ('&lt;key&gt;#&lt;epochId&gt;') into the innermost open scope; a no-op when no scope is
     * open (a demand that runs outside any update attempt has no progress marker to carry the ledger).
     */
    public static void recordConsumed(final String key, final String epochId) {
        final EpochScope scope = SCOPE.get();
        if (scope != null) {
            scope.consumedEntries.add(key + "#" + epochId);
        }
    }

    /**
     * The consumed epoch entries of the innermost open scope, or null when no scope is open (so that a legacy 6-field
     * marker stays byte-identical).
     */
    public static List<String> currentConsumedEntries() {
        final EpochScope scope = SCOPE.get();
        if (scope == null || scope.consumedEntries.isEmpty()) {
            return null;
        }
        return new ArrayList<>(scope.consumedEntries);
    }

    /**
     * True when the epoch is owned by this thread's demand tree (its own running attempt or one of its captured
     * ancestors) - the adoption gate for reading a staged epoch snapshot.
     */
    public static boolean isEpochVisibleToCurrentThread(final String epochId) {
        final EpochScope scope = SCOPE.get();
        return scope != null && scope.isEpochVisible(epochId);
    }

}
