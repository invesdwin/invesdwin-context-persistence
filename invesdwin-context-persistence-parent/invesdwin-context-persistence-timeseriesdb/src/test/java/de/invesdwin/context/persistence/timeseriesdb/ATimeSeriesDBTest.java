package de.invesdwin.context.persistence.timeseriesdb;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.TypeDelegateSerde;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;

// CHECKSTYLE:OFF
@NotThreadSafe
public class ATimeSeriesDBTest extends ATest {
    //CHECKSTYLE:ON

    @Test
    public void testGetPreviousAndNext() throws IncompleteUpdateFoundException {
        final String key = "asdf";
        final ATimeSeriesDB<String, FDate> table = new ATimeSeriesDB<String, FDate>("testGetPrevious") {

            @Override
            protected ISerde<FDate> newValueSerde() {
                return new TypeDelegateSerde<FDate>(FDate.class);
            }

            @Override
            protected Integer newValueFixedLength() {
                return null;
            }

            @Override
            protected String innerHashKeyToString(final String key) {
                return key;
            }

            @Override
            protected FDate extractEndTime(final FDate value) {
                return value;
            }

            @Override
            protected File getBaseDirectory() {
                return ContextProperties.TEMP_DIRECTORY;
            }
        };
        final List<FDate> dates = new ArrayList<>();
        for (int i = 2000; i <= 2010; i++) {
            dates.add(FDateBuilder.newDate(i));
        }
        new ATimeSeriesUpdater<String, FDate>(key, table) {

            @Override
            protected ICloseableIterable<? extends FDate> getSource(final FDate updateFrom) {
                return WrapperCloseableIterable.maybeWrap(dates);
            }

            @Override
            protected void onUpdateFinished(final Instant updateStart) {
            }

            @Override
            protected void onUpdateStart() {
            }

            @Override
            protected FDate extractEndTime(final FDate element) {
                return element;
            }

            @Override
            protected void onFlush(final int flushIndex,
                    final ATimeSeriesUpdater<String, FDate>.UpdateProgress updateProgress) {
            }

            @Override
            public Percent getProgress() {
                return null;
            }
        }.update();

        for (int i = 1; i < dates.size(); i++) {
            final FDate value = table.getPreviousValue(key, dates.get(dates.size() - 1), i);
            final FDate expectedValue = dates.get(dates.size() - i - 1);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 1; i < dates.size(); i++) {
            final FDate value = table.getPreviousValue(key, FDate.MAX_DATE, i);
            final FDate expectedValue = dates.get(dates.size() - i - 1);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 1; i < dates.size(); i++) {
            final FDate value = table.getPreviousValue(key, FDate.MIN_DATE, i);
            final FDate expectedValue = dates.get(0);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }

        for (int i = 1; i < dates.size(); i++) {
            final FDate value = table.getNextValue(key, dates.get(0), i);
            final FDate expectedValue = dates.get(i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 1; i < dates.size(); i++) {
            final FDate value = table.getNextValue(key, FDate.MIN_DATE, i);
            final FDate expectedValue = dates.get(i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 1; i < dates.size(); i++) {
            final FDate value = table.getNextValue(key, FDate.MAX_DATE, i);
            final FDate expectedValue = dates.get(dates.size() - 1);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
    }

    @Test
    public void testUpdateFile() throws IncompleteUpdateFoundException {
        final String key = "asdf";
        final ATimeSeriesDB<String, FDate> table = new ATimeSeriesDB<String, FDate>("testUpdateFile") {

            @Override
            protected ISerde<FDate> newValueSerde() {
                return new TypeDelegateSerde<FDate>(FDate.class);
            }

            @Override
            protected Integer newValueFixedLength() {
                return null;
            }

            @Override
            protected String innerHashKeyToString(final String key) {
                return key;
            }

            @Override
            protected FDate extractEndTime(final FDate value) {
                return value;
            }

            @Override
            protected File getBaseDirectory() {
                return ContextProperties.TEMP_DIRECTORY;
            }
        };
        final List<FDate> dates = new ArrayList<>();
        for (int i = 2000; i <= 2010; i++) {
            dates.add(FDateBuilder.newDate(i));
        }
        new ATimeSeriesUpdater<String, FDate>(key, table) {

            @Override
            protected ICloseableIterable<? extends FDate> getSource(final FDate updateFrom) {
                return WrapperCloseableIterable.maybeWrap(dates);
            }

            @Override
            protected void onUpdateFinished(final Instant updateStart) {
            }

            @Override
            protected void onUpdateStart() {
            }

            @Override
            protected FDate extractEndTime(final FDate element) {
                return element;
            }

            @Override
            protected void onFlush(final int flushIndex,
                    final ATimeSeriesUpdater<String, FDate>.UpdateProgress updateProgress) {
            }

            @Override
            public Percent getProgress() {
                return null;
            }
        }.update();

        final List<FDate> dates2 = new ArrayList<>();
        for (int i = 2010; i <= 2020; i++) {
            dates2.add(FDateBuilder.newDate(i));
        }

        final List<FDate> updateDates = new ArrayList<>();
        for (int i = 1990; i <= 2020; i++) {
            //update should skip earlier history
            updateDates.add(FDateBuilder.newDate(i));
        }

        new ATimeSeriesUpdater<String, FDate>(key, table) {

            @Override
            protected ICloseableIterable<? extends FDate> getSource(final FDate updateFrom) {
                return WrapperCloseableIterable.maybeWrap(updateDates);
            }

            @Override
            protected void onUpdateFinished(final Instant updateStart) {
            }

            @Override
            protected void onUpdateStart() {
            }

            @Override
            protected FDate extractEndTime(final FDate element) {
                return element;
            }

            @Override
            protected void onFlush(final int flushIndex,
                    final ATimeSeriesUpdater<String, FDate>.UpdateProgress updateProgress) {
            }

            @Override
            public Percent getProgress() {
                return null;
            }
        }.update();

        final List<FDate> allDates = new ArrayList<>();
        allDates.addAll(dates.subList(0, dates.size() - 1));
        allDates.addAll(dates2);

        final ICloseableIterable<FDate> rangeValues = table.rangeValues(key, FDate.MIN_DATE, FDate.MAX_DATE);
        int foundValues = 0;
        try (ICloseableIterator<FDate> it = rangeValues.iterator()) {
            while (true) {
                final FDate next = it.next();
                final FDate expected = allDates.get(foundValues);
                if (!next.equals(expected)) {
                    throw new IllegalArgumentException(
                            foundValues + ". expected [" + expected + "] found [" + next + "]");
                }
                foundValues++;
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
        Assertions.checkEquals(allDates.size(), foundValues);
    }

    @Test
    public void testGetPreviousAndNextSkipFile() throws IncompleteUpdateFoundException {
        final String key = "asdf";
        final ATimeSeriesDB<String, FDate> table = new ATimeSeriesDB<String, FDate>("testGetPreviousSkipFile") {

            @Override
            protected ISerde<FDate> newValueSerde() {
                return new TypeDelegateSerde<FDate>(FDate.class);
            }

            @Override
            protected Integer newValueFixedLength() {
                return null;
            }

            @Override
            protected String innerHashKeyToString(final String key) {
                return key;
            }

            @Override
            protected FDate extractEndTime(final FDate value) {
                return value;
            }

            @Override
            protected File getBaseDirectory() {
                return ContextProperties.TEMP_DIRECTORY;
            }
        };
        final List<FDate> dates = new ArrayList<>();
        for (int i = 0; i < 100_000; i++) {
            dates.add(new FDate(i));
        }
        final MutableInt segments = new MutableInt();
        new ATimeSeriesUpdater<String, FDate>(key, table) {

            @Override
            protected ICloseableIterable<? extends FDate> getSource(final FDate updateFrom) {
                return WrapperCloseableIterable.maybeWrap(dates);
            }

            @Override
            protected void onUpdateFinished(final Instant updateStart) {
            }

            @Override
            protected void onUpdateStart() {
            }

            @Override
            protected FDate extractEndTime(final FDate element) {
                return element;
            }

            @Override
            protected void onFlush(final int flushIndex,
                    final ATimeSeriesUpdater<String, FDate>.UpdateProgress updateProgress) {
                segments.increment();
            }

            @Override
            public Percent getProgress() {
                return null;
            }
        }.update();
        Assertions.assertThat(segments.intValue()).isEqualByComparingTo(10);

        for (int i = 0; i < dates.size(); i += ATimeSeriesUpdater.BATCH_FLUSH_INTERVAL) {
            final FDate expectedValue = dates.get(dates.size() - i - 1);
            final long expectedIndex = expectedValue.millisValue();
            final FDate value = table.getPreviousValue(key, dates.get(dates.size() - 1), i);
            final long valueIndex = value.millisValue();
            Assertions.checkEquals(valueIndex, expectedIndex,
                    i + ": expected [" + expectedIndex + "] got [" + valueIndex + "]");
        }
        for (int i = 0; i < dates.size(); i += ATimeSeriesUpdater.BATCH_FLUSH_INTERVAL) {
            final FDate expectedValue = dates.get(dates.size() - i - 1);
            final long expectedIndex = expectedValue.millisValue();
            final FDate value = table.getPreviousValue(key, FDate.MAX_DATE, i);
            final long valueIndex = value.millisValue();
            Assertions.checkEquals(valueIndex, expectedIndex,
                    i + ": expected [" + expectedIndex + "] got [" + valueIndex + "]");
        }
        for (int i = 0; i < dates.size(); i += ATimeSeriesUpdater.BATCH_FLUSH_INTERVAL) {
            final FDate expectedValue = dates.get(0);
            final long expectedIndex = expectedValue.millisValue();
            final FDate value = table.getPreviousValue(key, FDate.MIN_DATE, i);
            final long valueIndex = value.millisValue();
            Assertions.checkEquals(valueIndex, expectedIndex,
                    i + ": expected [" + expectedIndex + "] got [" + valueIndex + "]");
        }

        for (int i = 0; i < dates.size(); i += ATimeSeriesUpdater.BATCH_FLUSH_INTERVAL) {
            final FDate expectedValue = dates.get(i);
            final long expectedIndex = expectedValue.millisValue();
            final FDate value = table.getNextValue(key, dates.get(0), i);
            final long valueIndex = value.millisValue();
            Assertions.checkEquals(valueIndex, expectedIndex,
                    i + ": expected [" + expectedIndex + "] got [" + valueIndex + "]");
        }
        for (int i = 0; i < dates.size(); i += ATimeSeriesUpdater.BATCH_FLUSH_INTERVAL) {
            final FDate expectedValue = dates.get(i);
            final long expectedIndex = expectedValue.millisValue();
            final FDate value = table.getNextValue(key, FDate.MIN_DATE, i);
            final long valueIndex = value.millisValue();
            Assertions.checkEquals(valueIndex, expectedIndex,
                    i + ": expected [" + expectedIndex + "] got [" + valueIndex + "]");
        }
        for (int i = 0; i < dates.size(); i += ATimeSeriesUpdater.BATCH_FLUSH_INTERVAL) {
            final FDate expectedValue = dates.get(dates.size() - 1);
            final long expectedIndex = expectedValue.millisValue();
            final FDate value = table.getNextValue(key, FDate.MAX_DATE, i);
            final long valueIndex = value.millisValue();
            Assertions.checkEquals(valueIndex, expectedIndex,
                    i + ": expected [" + expectedIndex + "] got [" + valueIndex + "]");
        }

    }

}
