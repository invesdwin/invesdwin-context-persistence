package de.invesdwin.context.persistence.timeseriesdb.updater;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.time.date.FDate;

/**
 * Key convention for the two logical, data-derived properties a source table persists next to its recalculation
 * stamp when its content is reset and reloaded:
 * <ul>
 * <li>{@code SourceRevision.<scope>}: a monotonic counter, previous value plus one, bumped once per source
 * (re)load or reset of that scope - a logical replacement for comparing the wall-clock recalculation stamp,
 * because it is a property of the data and not of the machine that last rebuilt the store;</li>
 * <li>{@code SourceWatermark.<scope>}: the max data timestamp of the source range that was loaded at that point,
 * written only when it can be derived from the data at hand - an unknown watermark never overwrites a known
 * one.</li>
 * </ul>
 * The scope is the source family plus its identifying parts (for example {@code TimeBars.<instrumentId>}), so
 * every source family keeps its own counter inside the shared per-hashKey updater properties.
 *
 * <p>
 * Absence of a key is the sentinel-equivalent state: a fresh or an older store has never published a revision,
 * and the first read of an absent revision is zero (no key is written by a read). Old recalculation-stamp keys
 * are left untouched, so this is additive and downgrade-safe.
 *
 * <p>
 * Thread-safety: the read-modify-write is not atomic on its own. It is safe at the call sites because every
 * source reset and every update completion runs inside the updater's update lock - single-flight per store key
 * inside this JVM and serialized across JVMs by the store's heartbeat file lock - which is the same protocol
 * that already serializes the recalculation-stamp and last-date keys of that properties file.
 */
@ThreadSafe
public final class SourceRevisionSupport {

    public static final String KEY_SOURCE_REVISION_PREFIX = "SourceRevision.";
    public static final String KEY_SOURCE_WATERMARK_PREFIX = "SourceWatermark.";

    private SourceRevisionSupport() {}

    /**
     * Records that the source of this scope was reset and is being reloaded: the revision is bumped and the
     * watermark of the range that was loaded when the reset fired is stored when it is known. Returns the new
     * revision.
     */
    public static long recordSourceReset(final IProperties properties, final String scopeKey,
            final FDate reloadedRangeWatermark) {
        final long revision = bumpSourceRevision(properties, scopeKey);
        setSourceWatermark(properties, scopeKey, reloadedRangeWatermark);
        return revision;
    }

    /**
     * Read-modify-write of the persisted revision: previous value plus one, zero when absent. Must be called
     * under the update lock of the store these properties belong to.
     */
    public static long bumpSourceRevision(final IProperties properties, final String scopeKey) {
        final Long previous = properties.getLongOptional(getSourceRevisionKey(scopeKey));
        final long next = (previous == null ? 0L : previous.longValue()) + 1L;
        properties.setLong(getSourceRevisionKey(scopeKey), Long.valueOf(next));
        return next;
    }

    /**
     * The persisted revision of this scope, zero when the source never published one (fresh or older store).
     */
    public static long getSourceRevision(final IProperties properties, final String scopeKey) {
        final Long revision = properties.getLongOptional(getSourceRevisionKey(scopeKey));
        return revision == null ? 0L : revision.longValue();
    }

    /**
     * Stores the watermark of this scope only when it is known: a null watermark leaves the last known one
     * untouched instead of erasing it.
     */
    public static void setSourceWatermark(final IProperties properties, final String scopeKey, final FDate watermark) {
        if (watermark == null) {
            return;
        }
        properties.setDate(getSourceWatermarkKey(scopeKey), watermark);
    }

    /**
     * The persisted watermark of this scope, null when no max data timestamp was ever recorded for it.
     */
    public static FDate getSourceWatermark(final IProperties properties, final String scopeKey) {
        return properties.getDateOptional(getSourceWatermarkKey(scopeKey));
    }

    public static String getSourceRevisionKey(final String scopeKey) {
        return KEY_SOURCE_REVISION_PREFIX + scopeKey;
    }

    public static String getSourceWatermarkKey(final String scopeKey) {
        return KEY_SOURCE_WATERMARK_PREFIX + scopeKey;
    }

}
