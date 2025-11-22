package de.invesdwin.context.persistence.timeseriesdb.segmented.live;

import java.io.File;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.base.ABaseDBWithCacheTest;
import de.invesdwin.context.persistence.timeseriesdb.segmented.PeriodicalSegmentFinder;
import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseriesdb.segmented.finder.HistoricalCacheSegmentFinder;
import de.invesdwin.context.persistence.timeseriesdb.segmented.finder.ISegmentFinder;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.collections.iterable.skip.ASkippingIterable;
import de.invesdwin.util.collections.loadingcache.historical.AHistoricalCache;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.range.TimeRange;

// CHECKSTYLE:OFF
@NotThreadSafe
public class ALiveSegmentedTimeSeriesDBWithCacheTest extends ABaseDBWithCacheTest {
    //CHECKSTYLE:ON

    @Override
    protected void putNewEntity(final FDate newEntity) throws IncompleteUpdateRetryableException {
        ((ALiveSegmentedTimeSeriesDB<String, FDate>) table).putNextLiveValue(KEY, newEntity);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        table = new TestLiveSegmentedTimeSeriesDB(getClass().getSimpleName(), entities);
    }

    public static final class TestLiveSegmentedTimeSeriesDB extends ALiveSegmentedTimeSeriesDB<String, FDate> {
        private final ISegmentFinder segmentFinder;
        private FDate curTime = null;
        private final List<FDate> entities;

        public TestLiveSegmentedTimeSeriesDB(final String name, final List<FDate> entities) {
            super(name);
            this.entities = entities;
            final AHistoricalCache<TimeRange> segmentFinderCache = PeriodicalSegmentFinder
                    .newCache(new Duration(2, FTimeUnit.YEARS), false);
            this.segmentFinder = new HistoricalCacheSegmentFinder(segmentFinderCache, null);
            for (final FDate entity : entities) {
                putNextLiveValue(KEY, entity);
            }
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
        public FDate extractStartTime(final FDate value) {
            return value;
        }

        @Override
        public FDate extractEndTime(final FDate value) {
            return value;
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
        public FDate getFirstAvailableHistoricalSegmentFrom(final String key) {
            if (entities.isEmpty() || curTime == null) {
                return null;
            }
            final FDate firstTime = FDates.min(curTime, entities.get(0));
            final TimeRange firstSegment = segmentFinder.getCacheQuery().getValue(firstTime);
            if (firstSegment.getTo().isBeforeOrEqualTo(curTime)) {
                return firstSegment.getFrom();
            } else {
                return segmentFinder.getCacheQuery().getValue(firstSegment.getFrom().addMilliseconds(-1)).getFrom();
            }
        }

        @Override
        public FDate getLastAvailableHistoricalSegmentTo(final String key, final FDate updateTo) {
            if (entities.isEmpty() || curTime == null) {
                return null;
            }
            final TimeRange lastSegment = segmentFinder.getCacheQuery().getValue(curTime);
            if (lastSegment.getTo().isBeforeOrEqualTo(curTime)) {
                return lastSegment.getTo();
            } else {
                return segmentFinder.getCacheQuery().getValue(lastSegment.getFrom().addMilliseconds(-1)).getTo();
            }
        }

        @Override
        public boolean putNextLiveValue(final String key, final FDate nextLiveValue) {
            curTime = nextLiveValue;
            return super.putNextLiveValue(key, nextLiveValue);
        }

        @Override
        protected String getElementsName() {
            return "values";
        }
    }

}
