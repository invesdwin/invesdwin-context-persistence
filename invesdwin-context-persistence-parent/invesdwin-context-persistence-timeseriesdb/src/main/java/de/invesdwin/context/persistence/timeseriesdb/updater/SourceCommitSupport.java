package de.invesdwin.context.persistence.timeseriesdb.updater;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.time.date.FDate;

/**
 * Mint rule for the content-commit stamp: the single date-typed revision a store advertises as "the content
 * commit I was last built from". It is written only at content-commit points - a source reset or the completion
 * of the reload that follows it - never at read or decision time and never from a run latch. The value is a
 * date (so it stays comparable with wall-clock artefacts and legible on the wire) but it obeys a revision's
 * ordering:
 * <ul>
 * <li>{@code max(previous + 1ps, candidate)} - strictly after the previously persisted stamp even when two
 * commits start in the same millisecond, following the monotonisation idiom of
 * {@link de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFiles} for the epoch-local data
 * ids; FDate carries a true picosecond fraction and the properties format round-trips it;</li>
 * <li>propagated by copy - while the source's own commit receipt (the candidate) is ahead of everything seen
 * before, the stamp IS that receipt, exact to its picosecond fraction; only when the candidate lags (a wipe and
 * re-download of the same range, a rolled-back clock) does the +1ps floor take over, so a content change is
 * never invisible downstream;</li>
 * <li>absence is the sentinel-equivalent state - no previous and no candidate stays absent; neither a read nor
 * a commit with nothing to record ever advances or invents a stamp.</li>
 * </ul>
 *
 * <p>
 * Thread-safety: the read-modify-write is not atomic on its own. It is safe at the call sites because every
 * source reset and every update completion runs inside the updater's update lock - single-flight per store key
 * inside this JVM and serialized across JVMs by the store's heartbeat file lock - which is the same protocol
 * that already serializes the date keys of that properties file.
 */
@ThreadSafe
public final class SourceCommitSupport {

    private SourceCommitSupport() {}

    /**
     * The stamp floor: strictly after {@code previous}, one picosecond beyond it; null for no previous (the
     * first commit has no floor).
     */
    public static FDate nextToAfter(final FDate previous) {
        if (previous == null) {
            return null;
        }
        return previous.addPicoseconds(1);
    }

    /**
     * The commit stamp for this content commit given the previously persisted stamp and this commit's candidate
     * (the source's own commit receipt, or the wall clock where the source publishes none): equal to the
     * candidate while it is ahead of the previous stamp, else at least one picosecond past the previous stamp.
     * Null only when both are null - nothing to record.
     */
    public static FDate nextCommitStamp(final FDate previous, final FDate candidate) {
        if (previous == null) {
            return candidate;
        }
        final FDate floor = nextToAfter(previous);
        if (candidate == null || candidate.compareTo(floor) < 0) {
            return floor;
        }
        return candidate;
    }

    /**
     * Read-modify-write of the persisted stamp at {@code stampKey}: mints from the stored previous value and
     * this commit's candidate, stores the result (a null result leaves the key absent) and returns it. Must be
     * called under the update lock of the store these properties belong to.
     */
    public static FDate nextCommitStamp(final IProperties properties, final String stampKey, final FDate candidate) {
        final FDate next = nextCommitStamp(properties.getDateOptional(stampKey), candidate);
        properties.setDate(stampKey, next);
        return next;
    }

    /**
     * The persisted commit stamp, null when this store never published one (fresh or pre-stamp store). A read
     * never writes a key.
     */
    public static FDate getCommitStamp(final IProperties properties, final String stampKey) {
        return properties.getDateOptional(stampKey);
    }

}
