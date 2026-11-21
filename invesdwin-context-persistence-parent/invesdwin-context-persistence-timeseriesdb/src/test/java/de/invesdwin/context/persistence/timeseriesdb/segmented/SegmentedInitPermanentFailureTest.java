package de.invesdwin.context.persistence.timeseriesdb.segmented;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.retry.IRetryDisabledException;
import de.invesdwin.context.integration.retry.IRetryLaterException;
import de.invesdwin.context.integration.retry.RetryDisabledRuntimeException;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.segmented.finder.HistoricalCacheSegmentFinder;
import de.invesdwin.context.persistence.timeseriesdb.segmented.finder.ISegmentFinder;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.loadingcache.historical.AHistoricalCache;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.range.TimeRange;

/**
 * What a segment init does when the failure under the store's incomplete-update marker is permanent.
 * <p>
 * The store wraps a pass that could not finish in {@code IncompleteUpdateRetryableException} - "this attempt did not
 * complete, ask again later". The cache's handler used to answer that shape by wrapping whatever it caught in a
 * retry-later exception, which overrules the producer underneath: a permanent refusal becomes an unbounded re-ask. The
 * shape was measured on the consuming side of the same wrapper (cap35r, 2026-11-06): one demand whose store failure
 * arrived wrapped as incomplete re-asked 1,257 times over 51+ retries, with cumulative waits past PT34M, and the log
 * blamed the wait rather than the cause.
 * <p>
 * This cell drives the real demand path - {@code maybeInitSegmentSync} with a source that fails permanently - and
 * counts the passes, which is the quantity the fix is about. The source stops failing after the third pass on purpose:
 * a cell whose fixture depends on the code under test would otherwise spin until the runner kills the fork, and a
 * mutation probe that turns a guard into a hang is not a probe.
 * <p>
 * The other half of the handler - the {@code deleteRange} - was left alone after measuring it. Removing it changed
 * nothing observable: a segment whose init died is reset by the next attempt anyway (the INITIALIZING branch above the
 * init deletes range and status under the write lock and the heartbeat file lock). All the handler's own delete
 * controls is whether a reader can be served a PARTIALLY initialised segment in between, and the invariant on
 * {@code initSegmentWithStatusHandling} is that a wiped fresh state beats "being answered with the segment silently
 * missing".
 */
@NotThreadSafe
public class SegmentedInitPermanentFailureTest extends ATest {

    private static final int MAX_FAILING_PASSES = 3;

    @Test
    public void testAPermanentFailureUnderTheIncompleteMarkerStopsTheReAsk() throws Exception {
        final List<FDate> allValues = newValues(8);
        final FailingSourceDB db = new FailingSourceDB("permanentUnderMarker", allValues);
        try {
            final String key = "k";
            final ASegmentedTimeSeriesLookupStorageCache<String, FDate> cache = db.getSegmentedLookupTableCache(key);
            final TimeRange segment = db.getSegmentFinder(key).getCacheQuery().getValue(allValues.get(0));

            boolean completed = false;
            Throwable thrown = null;
            try {
                completed = cache.maybeInitSegmentSync(new SegmentedKey<>(key, segment),
                        segmentedKey -> db.newSource());
            } catch (final Throwable t) {
                thrown = t;
            }

            Assertions.checkTrue(thrown != null,
                "a permanent failure under the store's incomplete marker must reach the caller as a fault, not as a"
                        + " wait that the retry loop keeps eating. completed=" + Boolean.valueOf(completed)
                        + ", passes=" + Integer.valueOf(db.passes.get()));
            Assertions.checkTrue(Throwables.isCausedByType(thrown, IRetryDisabledException.class),
                    "the producer's declared permanence must still be the thing the retry engine reads: " + thrown);
            Assertions.checkTrue(!Throwables.isCausedByType(thrown, IRetryLaterException.class),
                    "the handler must not have re-labelled it as retry-later: " + thrown);
            //the quantity the fix exists for: one ask, not a retry storm
            Assertions.assertThat(db.passes.get()).as("pass count through the demand path").isEqualTo(1);
        } finally {
            db.close();
        }
    }

    private static List<FDate> newValues(final int count) {
        final List<FDate> values = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            values.add(FDateBuilder.newDate(2021, 3, 1).addDays(i));
        }
        return values;
    }

    /** A segmented store whose feed refuses permanently for the first {@code MAX_FAILING_PASSES} passes. */
    @NotThreadSafe
    private static final class FailingSourceDB extends ASegmentedTimeSeriesDB<String, FDate> {

        private final List<FDate> entities;
        private final ISegmentFinder segmentFinder;
        private final AtomicInteger passes = new AtomicInteger();

        private FailingSourceDB(final String name, final List<FDate> entities) {
            super(name);
            this.entities = entities;
            final AHistoricalCache<TimeRange> segmentFinderCache = PeriodicalSegmentFinder
                    .newCache(new Duration(2, FTimeUnit.YEARS), false);
            this.segmentFinder = new HistoricalCacheSegmentFinder(segmentFinderCache, null);
        }

        @Override
        protected int newBatchFlushInterval() {
            return 4;
        }

        private ICloseableIterable<FDate> newSource() {
            final int pass = passes.incrementAndGet();
            if (pass >= MAX_FAILING_PASSES) {
                //the fixture's own stop condition, so a mutant that keeps re-asking terminates instead of hanging
                return EmptyCloseableIterable.getInstance();
            }
            return new ICloseableIterable<FDate>() {

                @Override
                public ICloseableIterator<FDate> iterator() {
                    return new ICloseableIterator<FDate>() {

                        @Override
                        public boolean hasNext() {
                            return true;
                        }

                        @Override
                        public FDate next() {
                            //the shape a real refusal takes: a permanent verdict from the producer, which the store
                            //then wraps as "this pass did not finish"
                            throw new RetryDisabledRuntimeException(
                                    "this range can not be served by the source, permanently");
                        }

                        @Override
                        public void close() {
                            //nothing to release
                        }

                    };
                }

            };
        }

        @Override
        protected String getElementsName() {
            return "FailingSourceDB";
        }

        @Override
        protected ICloseableIterable<? extends FDate> downloadSegmentElements(final SegmentedKey<String> key) {
            return newSource();
        }

        @Override
        public ISegmentFinder getSegmentFinder(final String key) {
            return segmentFinder;
        }

        @Override
        protected ISerde<FDate> newValueSerde() {
            return FDateSerde.GET;
        }

        @Override
        protected Integer newValueFixedLength() {
            return FDateSerde.FIXED_LENGTH;
        }

        @Override
        public String innerHashKeyToString(final String key) {
            return key;
        }

        @Override
        public ITimeSeriesBaseDirectory getBaseDirectory() {
            return new TimeSeriesBaseDirectory(ContextProperties.TEMP_DIRECTORY);
        }

        @Override
        public FDate extractStartTime(final FDate value) {
            return value;
        }

        @Override
        public FDate extractEndTime(final FDate value) {
            return value;
        }

        @Override
        public FDate getFirstAvailableHistoricalSegmentFrom(final String key) {
            if (entities.isEmpty()) {
                return null;
            }
            return segmentFinder.getCacheQuery().getValue(entities.get(0)).getFrom();
        }

        @Override
        public FDate getLastAvailableHistoricalSegmentTo(final String key, final FDate updateTo) {
            if (entities.isEmpty()) {
                return null;
            }
            return segmentFinder.getCacheQuery().getValue(entities.get(entities.size() - 1)).getTo();
        }

    }

}
