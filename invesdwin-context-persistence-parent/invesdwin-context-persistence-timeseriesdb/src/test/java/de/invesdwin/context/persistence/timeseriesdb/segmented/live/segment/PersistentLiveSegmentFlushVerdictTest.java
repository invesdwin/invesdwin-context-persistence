package de.invesdwin.context.persistence.timeseriesdb.segmented.live.segment;

import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.retry.fast.FastNonBlockingRetryLaterRuntimeException;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseriesdb.segmented.finder.ISegmentFinder;
import de.invesdwin.context.persistence.timeseriesdb.segmented.live.ALiveSegmentedTimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.segmented.live.ALiveSegmentedTimeSeriesDB.HistoricalSegmentTable;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.IntegerSerde;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.range.TimeRange;

/**
 * What a live-segment flush must do when the range it was given yields nothing to write.
 * <p>
 * The production shape this pins: an idle update flushed a continuous leg's live segment, the writer reported no
 * end time, and the code asserted on it - an anonymous NullPointerException that the idle-updating listener cannot
 * retry, so every occurrence ended in an UnsubscribeFromEventException and dropped the instrument from automatic
 * updates. Measured on the live service: 66 flush failures and 68 unsubscribe records, all on continuous-chain legs.
 * The updater itself declares a null updatedTo permitted ("the callers tolerate a null updatedTo"), so the flush has
 * to answer with a named retryable verdict and keep the caller's buffer, not with an assertion.
 */
@NotThreadSafe
public class PersistentLiveSegmentFlushVerdictTest extends ATest {

    private final Map<Integer, FDate> extractTime = ILockCollectionFactory.getInstance(false).newMap();

    @Test
    public void anEmptyFlushIsARetryableVerdictAndKeepsTheSegmentEmpty() {
        final PersistentLiveSegment<FDate, Integer> segment = newSegment("emptyFlushVerdict");
        Assertions.assertThat(segment.isEmpty()).as("a fresh live segment is empty").isTrue();
        Assertions.assertThatExceptionOfType(FastNonBlockingRetryLaterRuntimeException.class)
                .as("nothing written must be a verdict the caller can retry, not an NPE it cannot classify")
                .isThrownBy(() -> segment.putNextLiveValues(EmptyCloseableIterable.getInstance()))
                .withMessageContaining("flush wrote nothing");
        Assertions.assertThat(segment.isEmpty())
                .as("a failed flush must not mark the segment written - the caller keeps its buffer and retries")
                .isTrue();
    }

    @Test
    public void aFlushAfterAFailedOneStillWritesTheBufferedValues() {
        final PersistentLiveSegment<FDate, Integer> segment = newSegment("emptyFlushThenRealOne");
        Assertions.assertThatExceptionOfType(FastNonBlockingRetryLaterRuntimeException.class)
                .isThrownBy(() -> segment.putNextLiveValues(EmptyCloseableIterable.getInstance()));
        final FDate valueTime = FDateBuilder.newDate(2000, 6, 15);
        extractTime.put(7, valueTime);
        segment.putNextLiveValues(WrapperCloseableIterable.maybeWrap(List.of(7)));
        Assertions.assertThat(segment.isEmpty()).as("the retry carries the values that survived the failed flush")
                .isFalse();
    }

    private PersistentLiveSegment<FDate, Integer> newSegment(final String name) {
        final SegmentedKey<FDate> segmentedKey = new SegmentedKey<>(FDates.MIN_DATE,
                new TimeRange(FDates.MIN_DATE, FDates.MAX_DATE));
        final ALiveSegmentedTimeSeriesDB<FDate, Integer> timeSeriesDB = new ALiveSegmentedTimeSeriesDB<FDate, Integer>(
                name) {

            @Override
            public ITimeSeriesBaseDirectory getBaseDirectory() {
                return new TimeSeriesBaseDirectory(ContextProperties.getCacheDirectory());
            }

            @Override
            protected ICloseableIterable<? extends Integer> downloadSegmentElements(
                    final SegmentedKey<FDate> key) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ISegmentFinder getSegmentFinder(final FDate key) {
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
            public FDate extractStartTime(final Integer value) {
                return extractEndTime(value);
            }

            @Override
            public FDate extractEndTime(final Integer value) {
                return extractTime.get(value);
            }

            @Override
            public String innerHashKeyToString(final FDate key) {
                return key.toString(FDate.FORMAT_UNDERSCORE_DATE_TIME_PS);
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
        return new PersistentLiveSegment<>(segmentedKey, historicalSegmentTable);
    }

}
