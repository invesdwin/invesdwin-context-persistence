package de.invesdwin.context.persistence.timeseriesdb.segmented;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.segmented.finder.HistoricalCacheSegmentFinder;
import de.invesdwin.context.persistence.timeseriesdb.segmented.finder.ISegmentFinder;
import de.invesdwin.context.persistence.timeseriesdb.segmented.live.ALiveSegmentedTimeSeriesDBWithCacheTest;
import de.invesdwin.context.persistence.timeseriesdb.segmented.status.VersionedTimeSeriesSegmentStatusTable;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.collections.iterable.skip.ATimeRangeSkippingIterable;
import de.invesdwin.util.collections.loadingcache.historical.AHistoricalCache;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.date.millis.FDateNanos;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.range.TimeRange;

// CHECKSTYLE:OFF
@NotThreadSafe
public class TimeSeriesCoverageTest extends ATest {
    //CHECKSTYLE:ON

    private static final String KEY = "asdf";

    @Test
    public void testCoverageFromSegmentStatusTableFixture() {
        final VersionedTimeSeriesSegmentStatusTable statusTable = newStatusTableFixture();
        final TimeRange year2020 = yearRange(2020);
        final TimeRange year2021 = yearRange(2021);
        final TimeRange year2022 = yearRange(2022);
        final TimeRange year2023 = yearRange(2023);

        statusTable.put(year2020, SegmentStatus.COMPLETE);
        statusTable.put(year2021, SegmentStatus.COMPLETE_EMPTY);
        statusTable.put(year2022, SegmentStatus.INITIALIZING);
        statusTable.put(year2023, SegmentStatus.COMPLETE);
        //the 'removed middle segment' case: status gone again, so the store declares nothing there
        statusTable.delete(year2023);

        final FDate queryFrom = FDateBuilder.newDate(2020, 6, 1);
        final FDate queryTo = FDateBuilder.newDate(2023, 6, 15);
        final TimeSeriesCoverage coverage = statusTable.coverage(queryFrom, queryTo);

        //COMPLETE: clamped to the query window at its start
        Assertions.assertThat(coverage.getCoveredRanges()).containsExactly(
                new TimeRange(queryFrom, year2020.getTo()));
        //COMPLETE_EMPTY: covered-no-data inside the window
        Assertions.assertThat(coverage.getCoveredEmptyRanges()).containsExactly(year2021);
        //INITIALIZING is not covered data: it shows as pending work, not as a covered range
        Assertions.assertThat(coverage.getInitializingRanges()).containsExactly(year2022);
        //the removed segment is an undeclared hole, localized exactly
        Assertions.assertThat(coverage.getGapRanges())
                .containsExactly(new TimeRange(year2023.getFrom(), queryTo));
        Assertions.assertThat(coverage.isFullyCovered()).isFalse();
        Assertions.assertThat(coverage.getQueryRange())
                .isEqualTo(new TimeRange(queryFrom, queryTo));
    }

    @Test
    public void testCoverageFullyCoveredAndAdjacentMerge() {
        final VersionedTimeSeriesSegmentStatusTable statusTable = newStatusTableFixture();
        final TimeRange year2020 = yearRange(2020);
        final TimeRange year2021 = yearRange(2021);
        statusTable.put(year2020, SegmentStatus.COMPLETE);
        statusTable.put(year2021, SegmentStatus.COMPLETE_EMPTY);

        //a window spanning both segments: adjacent same-status segments merge into one covered sub-range
        final FDate queryFrom = FDateBuilder.newDate(2020, 3, 1);
        final FDate queryTo = FDateBuilder.newDate(2021, 5, 31);
        final TimeSeriesCoverage coverage = statusTable.coverage(queryFrom, queryTo);
        Assertions.assertThat(coverage.getCoveredRanges()).containsExactly(new TimeRange(queryFrom, year2020.getTo()));
        Assertions.assertThat(coverage.getCoveredEmptyRanges())
                .containsExactly(new TimeRange(year2021.getFrom(), queryTo));
        Assertions.assertThat(coverage.getGapRanges()).isEmpty();
        Assertions.assertThat(coverage.getInitializingRanges()).isEmpty();
        Assertions.assertThat(coverage.isFullyCovered()).isTrue();

        //completely undeclared window: the whole window is one gap
        final FDate queryFrom2 = FDateBuilder.newDate(2022, 1, 1);
        final FDate queryTo2 = FDateBuilder.newDate(2022, 12, 31);
        final TimeSeriesCoverage coverage2 = statusTable.coverage(queryFrom2, queryTo2);
        Assertions.assertThat(coverage2.getCoveredRanges()).isEmpty();
        Assertions.assertThat(coverage2.getGapRanges()).containsExactly(new TimeRange(queryFrom2, queryTo2));
        Assertions.assertThat(coverage2.isFullyCovered()).isFalse();

        //null dates mean unlimited: everything declared is covered there
        final TimeSeriesCoverage coverage3 = statusTable.coverage(null, null);
        Assertions.assertThat(coverage3.getCoveredRanges()).containsExactly(year2020);
        Assertions.assertThat(coverage3.getCoveredEmptyRanges()).containsExactly(year2021);
        Assertions.assertThat(coverage3.getGapRanges()).hasSize(2);
    }

    @Test
    public void testCoverageViaSegmentedTimeSeriesDB() {
        final List<FDate> entities = new ArrayList<FDate>();
        for (int month = 1; month <= 15; month++) {
            final int year = 2020 + (month - 1) / 12;
            final int monthOfYear = (month - 1) % 12 + 1;
            entities.add(FDateBuilder.newDate(year, monthOfYear, 1));
        }
        final TestCoverageSegmentedTimeSeriesDB table = new TestCoverageSegmentedTimeSeriesDB(
                getClass().getSimpleName() + "_db", entities);
        try {
            for (final FDate entity : entities) {
                final ISegmentFinder segmentFinder = table.getSegmentFinder(KEY);
                final TimeRange segment = segmentFinder.getCacheQuery().getValue(segmentFinder.getDay(entity));
                table.getSegmentedLookupTableCache(KEY).maybeInitSegment(new SegmentedKey<String>(KEY, segment));
            }

            final FDate queryFrom = FDateBuilder.newDate(2020, 1, 1);
            final FDate queryTo = FDateBuilder.newDate(2021, 3, 31);
            TimeSeriesCoverage coverage = table.coverage(KEY, queryFrom, queryTo);
            Assertions.assertThat(coverage.isFullyCovered()).isTrue();
            Assertions.assertThat(coverage.getGapRanges()).isEmpty();
            //every month segment is COMPLETE and adjacent, so they merge into one covered sub-range
            Assertions.assertThat(coverage.getCoveredRanges()).containsExactly(new TimeRange(queryFrom, queryTo));

            //remove the status row of a middle segment: exactly that gap comes back
            final TimeRange hole = monthRange(2020, 7);
            table.getSegmentedLookupTableCache(KEY).getSegmentStatusTable().delete(hole);
            coverage = table.coverage(KEY, queryFrom, queryTo);
            Assertions.assertThat(coverage.getGapRanges()).containsExactly(hole);
            Assertions.assertThat(coverage.getCoveredRanges()).containsExactly(
                    new TimeRange(queryFrom, monthRange(2020, 6).getTo()),
                    new TimeRange(monthRange(2020, 8).getFrom(), queryTo));
            Assertions.assertThat(coverage.isFullyCovered()).isFalse();

            //declaring the same segment INITIALIZING: pending work instead of an undeclared hole
            table.getSegmentedLookupTableCache(KEY).getSegmentStatusTable().put(hole, SegmentStatus.INITIALIZING);
            coverage = table.coverage(KEY, queryFrom, queryTo);
            Assertions.assertThat(coverage.getGapRanges()).isEmpty();
            Assertions.assertThat(coverage.getInitializingRanges()).containsExactly(hole);
            Assertions.assertThat(coverage.isFullyCovered()).isFalse();

            //COMPLETE_EMPTY: covered-no-data, the window is fully declared again
            table.getSegmentedLookupTableCache(KEY).getSegmentStatusTable().put(hole, SegmentStatus.COMPLETE_EMPTY);
            coverage = table.coverage(KEY, queryFrom, queryTo);
            Assertions.assertThat(coverage.getGapRanges()).isEmpty();
            Assertions.assertThat(coverage.getInitializingRanges()).isEmpty();
            Assertions.assertThat(coverage.getCoveredEmptyRanges()).containsExactly(hole);
            Assertions.assertThat(coverage.isFullyCovered()).isTrue();
        } finally {
            table.close();
        }
    }

    @Test
    public void testCoverageLiveStoreIncludesLiveSegment() {
        final List<FDate> entities = new ArrayList<FDate>();
        //months Jun-2020 .. Mar-2021: all stay in the not-yet-converted live segment [2020,2021]
        for (int i = 0; i < 10; i++) {
            entities.add(FDateBuilder.newDate(2020, 6, 1).addMonths(i));
        }
        final ALiveSegmentedTimeSeriesDBWithCacheTest.TestLiveSegmentedTimeSeriesDB table = new ALiveSegmentedTimeSeriesDBWithCacheTest.TestLiveSegmentedTimeSeriesDB(
                getClass().getSimpleName() + "_live", entities);
        try {
            final FDate queryFrom = FDateBuilder.newDate(2020, 6, 1);
            final FDate lastLiveValue = entities.get(entities.size() - 1);
            //historical COMPLETE [2020,2021] merges with the live [2022,2023] data up to its last value
            final TimeSeriesCoverage coverage = table.coverage(KEY, queryFrom, lastLiveValue);
            Assertions.assertThat(coverage.isFullyCovered()).isTrue();
            Assertions.assertThat(coverage.getGapRanges()).isEmpty();
            Assertions.assertThat(coverage.getCoveredRanges()).containsExactly(new TimeRange(queryFrom, lastLiveValue));

            //past the last live value of the live segment the store declares nothing yet: localized tail gap
            final FDate queryTo = FDateBuilder.newDate(2022, 3, 31);
            final TimeSeriesCoverage coverage2 = table.coverage(KEY, queryFrom, queryTo);
            Assertions.assertThat(coverage2.getCoveredRanges())
                    .containsExactly(new TimeRange(queryFrom, lastLiveValue));
            Assertions.assertThat(coverage2.getGapRanges())
                    .containsExactly(new TimeRange(lastLiveValue.addPicoseconds(1), queryTo));
            Assertions.assertThat(coverage2.isFullyCovered()).isFalse();
        } finally {
            table.close();
        }
    }

    /**
     * An inverted query range (from after to) must never be reported as covered: the declared COMPLETE segments
     * intersect nothing for it, the buckets stay empty, and the inverted request itself comes back as the single
     * gap, so isFullyCovered() is false - the safe direction of the audit contract.
     */
    @Test
    public void testInvertedQueryRangeNeverClaimsFullCoverage() {
        final VersionedTimeSeriesSegmentStatusTable statusTable = newStatusTableFixture();
        statusTable.put(yearRange(2020), SegmentStatus.COMPLETE);
        statusTable.put(yearRange(2021), SegmentStatus.COMPLETE);
        statusTable.put(yearRange(2022), SegmentStatus.COMPLETE);
        statusTable.put(yearRange(2023), SegmentStatus.COMPLETE);

        final FDate queryFrom = FDateBuilder.newDate(2023, 6, 15);
        final FDate queryTo = FDateBuilder.newDate(2020, 6, 1);
        final TimeSeriesCoverage coverage = statusTable.coverage(queryFrom, queryTo);
        Assertions.assertThat(coverage.getCoveredRanges()).isEmpty();
        Assertions.assertThat(coverage.getCoveredEmptyRanges()).isEmpty();
        Assertions.assertThat(coverage.getInitializingRanges()).isEmpty();
        Assertions.assertThat(coverage.getGapRanges()).containsExactly(new TimeRange(queryFrom, queryTo));
        Assertions.assertThat(coverage.isFullyCovered()).isFalse();
    }

    private static VersionedTimeSeriesSegmentStatusTable newStatusTableFixture() {
        final File directory = new File(ContextProperties.TEMP_DIRECTORY,
                TimeSeriesCoverageTest.class.getSimpleName() + "_" + FDateNanos.elapsedNanos());
        directory.mkdirs();
        return new VersionedTimeSeriesSegmentStatusTable(new File(directory, "update.finished"), directory, 1);
    }

    private static TimeRange yearRange(final int year) {
        return new TimeRange(FDateBuilder.newDate(year, 1, 1),
                FDateBuilder.newDate(year + 1, 1, 1).addPicoseconds(-1));
    }

    private static TimeRange monthRange(final int year, final int month) {
        return new TimeRange(FDateBuilder.newDate(year, month, 1),
                FDateBuilder.newDate(month == 12 ? year + 1 : year, month == 12 ? 1 : month + 1, 1)
                        .addPicoseconds(-1));
    }

    public static final class TestCoverageSegmentedTimeSeriesDB extends ASegmentedTimeSeriesDB<String, FDate> {
        private final ISegmentFinder segmentFinder;
        private final List<FDate> entities;

        public TestCoverageSegmentedTimeSeriesDB(final String name, final List<FDate> entities) {
            super(name);
            this.entities = entities;
            final AHistoricalCache<TimeRange> segmentFinderCache = PeriodicalSegmentFinder
                    .newCache(new Duration(1, FTimeUnit.MONTHS), false);
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
        public String innerHashKeyToString(final String key) {
            return key;
        }

        @Override
        public ITimeSeriesBaseDirectory getBaseDirectory() {
            return new TimeSeriesBaseDirectory(ContextProperties.TEMP_DIRECTORY);
        }

        @Override
        protected ICloseableIterable<? extends FDate> downloadSegmentElements(final SegmentedKey<String> segmentedKey) {
            return new ATimeRangeSkippingIterable<FDate>(WrapperCloseableIterable.maybeWrap(entities),
                    segmentedKey.getSegment()) {
                @Override
                protected FDate extractEndTime(final FDate element) {
                    return element;
                }

                @Override
                protected String getName() {
                    return "TimeSeriesCoverageTest.downloadSegmentElements";
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
