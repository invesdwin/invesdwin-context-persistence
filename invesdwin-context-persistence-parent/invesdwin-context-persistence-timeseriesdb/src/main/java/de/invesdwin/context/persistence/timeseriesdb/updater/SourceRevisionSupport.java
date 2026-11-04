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
    /**
     * Prefix of the decision-side counterpart: the source revision a derived store was built from, persisted in
     * the derived store's own per-hashKey updater properties under this prefix plus the derived store key.
     */
    public static final String KEY_DERIVED_SOURCE_REVISION_PREFIX = "DerivedSourceRevision.";

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

    /**
     * The persisted revision of this scope, or null when the source never published one - the absence that lets
     * a consumer fall back to the stamp comparison instead of comparing against a made-up zero.
     */
    public static Long getSourceRevisionOptional(final IProperties properties, final String scopeKey) {
        return properties.getLongOptional(getSourceRevisionKey(scopeKey));
    }

    /**
     * The source revision a derived store recorded itself as built from, null when it never recorded one (a
     * derived store predating the revision-aware decision).
     */
    public static Long getDerivedSourceRevisionOptional(final IProperties properties, final String derivedKey) {
        return properties.getLongOptional(getDerivedSourceRevisionKey(derivedKey));
    }

    /**
     * Records which source revision this derived store was built from, next to its own recalculation stamp, so
     * the next decision can compare revisions instead of stamps.
     */
    public static void setDerivedSourceRevision(final IProperties properties, final String derivedKey,
            final long revision) {
        properties.setLong(getDerivedSourceRevisionKey(derivedKey), Long.valueOf(revision));
    }

    /**
     * The revision-aware rebuild decision of a derived store: TRUE means the source changed (full rebuild), FALSE
     * means the source is the same revision the store was built from (no rebuild, even when the wall-clock
     * recalculation stamps differ - stamps are a property of the machine that last rebuilt the source, not of the
     * data). Returns null when no verdict is possible: the source publishes no revision (a store predating the
     * dual-write), so the caller must fall back to the stamp comparison it uses today.
     */
    public static Boolean decideRevisionBasedRecalculation(final Long sourceRevision,
            final Long storedDerivedRevision) {
        if (sourceRevision == null) {
            return null;
        }
        if (storedDerivedRevision == null) {
            //never built under revision awareness: sentinel-equivalent, one rebuild, then steady state
            return Boolean.TRUE;
        }
        return Boolean.valueOf(storedDerivedRevision.longValue() != sourceRevision.longValue());
    }

    public static String getSourceRevisionKey(final String scopeKey) {
        return KEY_SOURCE_REVISION_PREFIX + scopeKey;
    }

    public static String getSourceWatermarkKey(final String scopeKey) {
        return KEY_SOURCE_WATERMARK_PREFIX + scopeKey;
    }

    public static String getDerivedSourceRevisionKey(final String derivedKey) {
        return KEY_DERIVED_SOURCE_REVISION_PREFIX + derivedKey;
    }

}
