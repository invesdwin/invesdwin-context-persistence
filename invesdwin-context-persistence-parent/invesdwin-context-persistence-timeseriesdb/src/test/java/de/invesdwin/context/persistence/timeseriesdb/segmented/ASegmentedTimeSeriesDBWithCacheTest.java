package de.invesdwin.context.persistence.timeseriesdb.segmented;

import java.io.File;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.base.ABaseDBWithCacheTest;
import de.invesdwin.context.persistence.timeseriesdb.segmented.finder.HistoricalCacheSegmentFinder;
import de.invesdwin.context.persistence.timeseriesdb.segmented.finder.ISegmentFinder;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.collections.iterable.skip.ASkippingIterable;
import de.invesdwin.util.collections.loadingcache.historical.AHistoricalCache;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.range.TimeRange;

// CHECKSTYLE:OFF
@NotThreadSafe
public class ASegmentedTimeSeriesDBWithCacheTest extends ABaseDBWithCacheTest {
    //CHECKSTYLE:ON

    @Override
    protected void putNewEntity(final FDate newEntity) throws IncompleteUpdateRetryableException {
        //noop
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        table = new TestSegmentedTimeSeriesDB(getClass().getSimpleName(), entities);
    }

    public static final class TestSegmentedTimeSeriesDB extends ASegmentedTimeSeriesDB<String, FDate> {
        private final ISegmentFinder segmentFinder;
        private final List<FDate> entities;

        public TestSegmentedTimeSeriesDB(final String name, final List<FDate> entities) {
            super(name);
            this.entities = entities;
            final AHistoricalCache<TimeRange> segmentFinderCache = PeriodicalSegmentFinder
                    .newCache(new Duration(2, FTimeUnit.YEARS), false);
            this.segmentFinder = new HistoricalCacheSegmentFinder(segmentFinderCache, null);
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
        protected String innerHashKeyToString(final String key) {
            return key;
        }

        @Override
        public File getBaseDirectory() {
            return ContextProperties.TEMP_DIRECTORY;
        }

        @Override
        protected ICloseableIterable<? extends FDate> downloadSegmentElements(final SegmentedKey<String> segmentedKey) {
            return new ASkippingIterable<FDate>(WrapperCloseableIterable.maybeWrap(entities)) {
                private final FDate from = segmentedKey.getSegment().getFrom();
                private final FDate to = segmentedKey.getSegment().getTo();

                @Override
                protected boolean skip(final FDate element) {
                    return element.isBefore(from) || element.isAfter(to);
                }
            };
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

        @Override
        protected String getElementsName() {
            return "values";
        }
    }

}
