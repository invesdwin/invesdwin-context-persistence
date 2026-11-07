package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.time.date.FDate;

/**
 * The anomaly branch of the redo of a standalone '_incomplete_' last file. That redo drops the file's last value and
 * asks the source from that value's end time, expecting it back; normally it gets it back (the pass replays the same
 * input and regenerates the bar, so the source's answer replaces the published one blindly - unchanged semantics).
 * The anomaly is the source not serving that position at all: its answer starts behind it, or it runs dry. For a value
 * the store's validated-final watermark already seals, losing it there would delete published content on nothing but
 * the supply's silence, and this iterable keeps it.
 * <p>
 * A source answer for exactly the dropped position wins, a source answer for a LATER position gets the published value
 * re-written in front of it (chain order preserved), and a source that never answers the position gets the published
 * value appended. The written chain is thereby a function of the previous generation plus what the source answered,
 * not of the fact that a redo happened to drop a value it could not get back.
 */
@NotThreadSafe
final class RedoTailPreservingIterable<V> implements ICloseableIterable<V> {

    private final ICloseableIterable<? extends V> delegate;
    private final V redoDroppedTailValue;
    private final FDate redoDroppedTailEndTime;
    private final Function<V, FDate> extractEndTime;

    RedoTailPreservingIterable(final ICloseableIterable<? extends V> delegate, final V redoDroppedTailValue,
            final FDate redoDroppedTailEndTime, final Function<V, FDate> extractEndTime) {
        this.delegate = delegate;
        this.redoDroppedTailValue = redoDroppedTailValue;
        this.redoDroppedTailEndTime = redoDroppedTailEndTime;
        this.extractEndTime = extractEndTime;
    }

    @Override
    public ICloseableIterator<V> iterator() {
        return new ICloseableIterator<V>() {

            private final ICloseableIterator<? extends V> it = delegate.iterator();
            private V cachedNext;
            //the source answer held back while the published tail goes in front of it
            private V pendingSourceAnswer;
            //true once the dropped position is settled: answered by the source, or re-published
            private boolean tailSettled;

            @Override
            public boolean hasNext() {
                if (cachedNext == null) {
                    cachedNext = readNext();
                }
                return cachedNext != null;
            }

            @Override
            public V next() {
                if (!hasNext()) {
                    throw FastNoSuchElementException.getInstance(
                            "RedoTailPreservingIterable: end reached");
                }
                final V next = cachedNext;
                cachedNext = null;
                return next;
            }

            private V readNext() {
                while (true) {
                    final V candidate;
                    if (pendingSourceAnswer != null) {
                        candidate = pendingSourceAnswer;
                        pendingSourceAnswer = null;
                    } else if (it.hasNext()) {
                        candidate = it.next();
                    } else {
                        candidate = null;
                    }
                    if (candidate == null) {
                        if (!tailSettled) {
                            //the source never answered the dropped position: keep the published value
                            tailSettled = true;
                            return redoDroppedTailValue;
                        }
                        return null;
                    }
                    if (tailSettled) {
                        return candidate;
                    }
                    final FDate candidateEndTime = extractEndTime.apply(candidate);
                    if (candidateEndTime.equalsNotNullSafe(redoDroppedTailEndTime)) {
                        //the source answered that position: its answer replaces the published one
                        tailSettled = true;
                        return candidate;
                    }
                    if (candidateEndTime.isAfterNotNullSafe(redoDroppedTailEndTime)) {
                        //the source skipped the position and answered behind it: re-publish the tail first
                        tailSettled = true;
                        pendingSourceAnswer = candidate;
                        return redoDroppedTailValue;
                    }
                    //a value in front of the dropped position (the re-written prefix): pass it through unchanged
                    return candidate;
                }
            }

            @Override
            public void close() {
                it.close();
            }
        };
    }
}
