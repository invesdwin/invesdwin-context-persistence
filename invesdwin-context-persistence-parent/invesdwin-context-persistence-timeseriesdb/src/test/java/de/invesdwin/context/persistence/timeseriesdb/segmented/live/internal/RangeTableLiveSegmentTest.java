package de.invesdwin.context.persistence.timeseriesdb.segmented.live.internal;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import org.assertj.core.api.Fail;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseriesdb.segmented.live.ALiveSegmentedTimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.segmented.live.ALiveSegmentedTimeSeriesDB.HistoricalSegmentTable;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.loadingcache.historical.AHistoricalCache;
import de.invesdwin.util.concurrent.lock.disabled.DisabledLock;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.IntegerSerde;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.range.TimeRange;

@NotThreadSafe
public class RangeTableLiveSegmentTest extends ATest {

    @Test
    public void testInverseOrder() {
        final Map<Integer, FDate> extractTime = new HashMap<>();
        final SegmentedKey<FDate> segmentedKey = new SegmentedKey<FDate>(FDate.MIN_DATE,
                new TimeRange(FDate.MIN_DATE, FDate.MAX_DATE));
        final ALiveSegmentedTimeSeriesDB<FDate, Integer> timeSeriesDB = new ALiveSegmentedTimeSeriesDB<FDate, Integer>(
                "testInverseOrder") {

            @Override
            protected File getBaseDirectory() {
                return ContextProperties.getCacheDirectory();
            }

            @Override
            protected ICloseableIterable<? extends Integer> downloadSegmentElements(
                    final SegmentedKey<FDate> segmentedKey) {
                throw new UnsupportedOperationException();
            }

            @Override
            public AHistoricalCache<TimeRange> getSegmentFinder(final FDate key) {
                throw new UnsupportedOperationException();
            }

            @Override
            protected Integer newValueFixedLength() {
                return null;
            }

            @Override
            protected ISerde<Integer> newValueSerde() {
                return IntegerSerde.GET;
            }

            @Override
            protected FDate extractEndTime(final Integer value) {
                return extractTime.get(value);
            }

            @Override
            protected String innerHashKeyToString(final FDate key) {
                return key.toString(FDate.FORMAT_UNDERSCORE_DATE_TIME_MS);
            }

            @Override
            public FDate getFirstAvailableHistoricalSegmentFrom(final FDate key) {
                throw new UnsupportedOperationException();
            }

            @Override
            public FDate getLastAvailableHistoricalSegmentTo(final FDate key, final FDate updateTo) {
                throw new UnsupportedOperationException();
            }

            @Override
            protected String getElementsName() {
                throw new UnsupportedOperationException();
            }

        };
        @SuppressWarnings("unchecked")
        final ALiveSegmentedTimeSeriesDB<FDate, Integer>.HistoricalSegmentTable historicalSegmentTable = Reflections
                .field("historicalSegmentTable")
                .ofType(HistoricalSegmentTable.class)
                .in(timeSeriesDB)
                .get();
        final RangeTableLiveSegment<FDate, Integer> rangeTable = new RangeTableLiveSegment<FDate, Integer>(segmentedKey,
                historicalSegmentTable);
        final FDate now = FDateBuilder.newDate(2000);
        final FDate oneDate = now.addDays(1);
        final FDate twoDate = now.addDays(2);
        final FDate threeDate = now.addDays(3);
        rangeTable.putNextLiveValue(oneDate, 1);
        extractTime.put(1, oneDate);
        rangeTable.putNextLiveValue(twoDate, 2);
        extractTime.put(2, twoDate);
        rangeTable.putNextLiveValue(threeDate, 3);
        extractTime.put(3, threeDate);
        final ICloseableIterator<Integer> range3 = rangeTable.rangeValues(now, null, DisabledLock.INSTANCE, null)
                .iterator();
        Assertions.assertThat(range3.next()).isEqualTo(1);
        Assertions.assertThat(range3.next()).isEqualTo(2);
        Assertions.assertThat(range3.next()).isEqualTo(3);
        Assertions.assertThat(range3.hasNext()).isFalse();
        try {
            range3.next();
            Fail.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assertions.assertThat(e).isNotNull();
        }
        range3.close(); //should already be closed but should not cause an error when calling again

        final ICloseableIterator<Integer> rangeNone = rangeTable.rangeValues(null, null, DisabledLock.INSTANCE, null)
                .iterator();
        Assertions.assertThat(rangeNone.next()).isEqualTo(1);
        Assertions.assertThat(rangeNone.next()).isEqualTo(2);
        Assertions.assertThat(rangeNone.next()).isEqualTo(3);
        Assertions.assertThat(rangeNone.hasNext()).isFalse();
        try {
            rangeNone.next();
            Fail.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assertions.assertThat(e).isNotNull();
        }

        final ICloseableIterator<Integer> rangeMin = rangeTable
                .rangeValues(FDate.MIN_DATE, null, DisabledLock.INSTANCE, null)
                .iterator();
        Assertions.assertThat(rangeMin.next()).isEqualTo(1);
        Assertions.assertThat(rangeMin.next()).isEqualTo(2);
        Assertions.assertThat(rangeMin.next()).isEqualTo(3);
        Assertions.assertThat(rangeMin.hasNext()).isFalse();
        try {
            rangeMin.next();
            Fail.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assertions.assertThat(e).isNotNull();
        }

        final ICloseableIterator<Integer> rangeMax = rangeTable
                .rangeValues(FDate.MAX_DATE, null, DisabledLock.INSTANCE, null)
                .iterator();
        Assertions.assertThat(rangeMax.hasNext()).isFalse();
        try {
            rangeMax.next();
            Fail.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assertions.assertThat(e).isNotNull();
        }

        final ICloseableIterator<Integer> range2 = rangeTable.rangeValues(twoDate, null, DisabledLock.INSTANCE, null)
                .iterator();
        Assertions.assertThat(range2.next()).isEqualTo(2);
        Assertions.assertThat(range2.next()).isEqualTo(3);
        Assertions.assertThat(range2.hasNext()).isFalse();
        try {
            range2.next();
            Fail.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assertions.assertThat(e).isNotNull();
        }

        testReverse(rangeTable, oneDate, twoDate, threeDate);

        testGetLatestForRange(rangeTable, oneDate, twoDate, threeDate);

        rangeTable.close();
    }

    private void testGetLatestForRange(final ILiveSegment<FDate, Integer> rangeTable, final FDate oneDate,
            final FDate twoDate, final FDate threeDate) {
        Assertions.assertThat(rangeTable.getLatestValue(oneDate)).isEqualTo(1);
        Assertions.assertThat(rangeTable.getLatestValue(twoDate)).isEqualTo(2);
        Assertions.assertThat(rangeTable.getLatestValue(threeDate)).isEqualTo(3);

        Assertions.assertThat(rangeTable.getLatestValue(oneDate.addMilliseconds(-1))).isEqualTo(1);
        Assertions.assertThat(rangeTable.getLatestValue(twoDate.addMilliseconds(-1))).isEqualTo(1);
        Assertions.assertThat(rangeTable.getLatestValue(threeDate.addMilliseconds(-1))).isEqualTo(2);
        Assertions.assertThat(rangeTable.getLatestValue(threeDate.addMilliseconds(1))).isEqualTo(3);
        Assertions.assertThat(rangeTable.getLatestValue(threeDate.addDays(1))).isEqualTo(3);

        Assertions.assertThat(rangeTable.getLatestValue(FDate.MIN_DATE)).isEqualTo(1);
        Assertions.assertThat(rangeTable.getLatestValue(FDate.MAX_DATE)).isEqualTo(3);
    }

    private void testReverse(final ILiveSegment<FDate, Integer> rangeTable, final FDate oneFDate, final FDate twoFDate,
            final FDate threeFDate) {
        final ICloseableIterator<Integer> range3Reverse = rangeTable
                .rangeReverseValues(threeFDate, null, DisabledLock.INSTANCE, null)
                .iterator();
        Assertions.assertThat(range3Reverse.next()).isEqualTo(3);
        Assertions.assertThat(range3Reverse.next()).isEqualTo(2);
        Assertions.assertThat(range3Reverse.next()).isEqualTo(1);
        Assertions.assertThat(range3Reverse.hasNext()).isFalse();
        try {
            range3Reverse.next();
            Fail.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assertions.assertThat(e).isNotNull();
        }

        final ICloseableIterator<Integer> rangeNoneReverse = rangeTable
                .rangeReverseValues(null, null, DisabledLock.INSTANCE, null)
                .iterator();
        Assertions.assertThat(rangeNoneReverse.next()).isEqualTo(3);
        Assertions.assertThat(rangeNoneReverse.next()).isEqualTo(2);
        Assertions.assertThat(rangeNoneReverse.next()).isEqualTo(1);
        Assertions.assertThat(rangeNoneReverse.hasNext()).isFalse();
        try {
            rangeNoneReverse.next();
            Fail.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assertions.assertThat(e).isNotNull();
        }

        final ICloseableIterator<Integer> range2Reverse = rangeTable
                .rangeReverseValues(twoFDate, null, DisabledLock.INSTANCE, null)
                .iterator();
        Assertions.assertThat(range2Reverse.next()).isEqualTo(2);
        Assertions.assertThat(range2Reverse.next()).isEqualTo(1);
        Assertions.assertThat(range2Reverse.hasNext()).isFalse();
        try {
            range2Reverse.next();
            Fail.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assertions.assertThat(e).isNotNull();
        }

        final ICloseableIterator<Integer> range32Reverse = rangeTable
                .rangeReverseValues(threeFDate, twoFDate, DisabledLock.INSTANCE, null)
                .iterator();
        Assertions.assertThat(range32Reverse.next()).isEqualTo(3);
        Assertions.assertThat(range32Reverse.next()).isEqualTo(2);
        Assertions.assertThat(range32Reverse.hasNext()).isFalse();
        try {
            range32Reverse.next();
            Fail.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assertions.assertThat(e).isNotNull();
        }

        final ICloseableIterator<Integer> range21Reverse = rangeTable
                .rangeReverseValues(twoFDate, oneFDate, DisabledLock.INSTANCE, null)
                .iterator();
        Assertions.assertThat(range21Reverse.next()).isEqualTo(2);
        Assertions.assertThat(range21Reverse.next()).isEqualTo(1);
        Assertions.assertThat(range21Reverse.hasNext()).isFalse();
        try {
            range21Reverse.next();
            Fail.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assertions.assertThat(e).isNotNull();
        }
    }

}
