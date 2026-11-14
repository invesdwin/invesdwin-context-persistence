package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdateProgress;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.time.date.FDates;

/**
 * The in-pass order guard (the progress's running maximum) is deliberately per batch: it is cleared at every batch
 * boundary, so within one pass it only refuses an element that starts before the PREVIOUS element's end. A published
 * file carries many batches, and the file chain is validated first-value against first-value - so the thing that has
 * to catch a batch junction is a check that compares the previous batch's LAST value with the next batch's FIRST
 * value. This test feeds exactly the shape a live consumer choked on: one pass, batch1 ascending, batch2 restarting
 * strictly behind batch1's last value. It pins that the junction IS refused before any of it can be served - the
 * refusal site being the per-junction last-value/first-value assertion of the update transaction, not the in-pass
 * guard.
 */
@NotThreadSafe
public class TimeSeriesBatchJunctionOrderTest extends ATest {

    private static final int BATCH_FLUSH_INTERVAL = 10;
    private static final String KEY = "batchJunctionKey";

    private File baseDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY,
                TimeSeriesBatchJunctionOrderTest.class.getSimpleName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Files.deleteQuietly(baseDirectory);
    }

    /**
     * The shape itself: batch1 fills exactly one full batch ascending, batch2 starts behind batch1's last value and
     * continues from there. Either the pass refuses the junction (the invariant never reaches the reader) or it
     * publishes it and rangeValues() must still come out ascending - never a value older than the one before it.
     */
    @Test
    public void testRangeValuesNeverServeAValueOlderThanTheOneBeforeThem() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("batchJunction");

        //batch1: 12:00..12:09, one value per minute, exactly BATCH_FLUSH_INTERVAL values -> one full batch
        final List<FDate> batch1 = newValues(0, BATCH_FLUSH_INTERVAL - 1);
        //batch2 starts at 12:05 - strictly behind batch1's last value (12:09) - and runs to 12:20
        final List<FDate> batch2 = newValues(BATCH_FLUSH_INTERVAL / 2, 20);
        Assertions.checkTrue(batch2.get(0).isBeforeNotNullSafe(batch1.get(batch1.size() - 1)),
                "the scenario needs batch2 to start behind batch1's end: " + batch2.get(0) + " vs "
                        + batch1.get(batch1.size() - 1));

        final List<FDate> source = new ArrayList<>(batch1);
        source.addAll(batch2);

        final Throwable thrown = Assertions.catchThrowable(() -> {
            newUpdater(table, source).update().close();
        });

        //the junction must be refused by the update - an inverted batch junction that reaches a file is what a live
        //consumer downstream reads back as a record older than one it already served
        Assertions.assertThat(thrown).as("batch-junction inversion must not be accepted").isNotNull();
        Assertions.assertThat(Throwables.isCausedByMessagePart(thrown, "should not be after firstValueTime"))
                .as("the refusal must be the per-junction last-value/first-value check, not an unrelated failure: "
                        + thrown)
                .isTrue();

        final List<FDate> served = readValues(table, KEY);
        for (int i = 1; i < served.size(); i++) {
            Assertions.checkFalse(served.get(i - 1).isAfterNotNullSafe(served.get(i)),
                    "rangeValues() served a value older than the one before it at position " + i + " [" + served.get(i)
                            + "] after [" + served.get(i - 1) + "] - a batch-junction inversion was published and"
                            + " served: " + served);
        }
    }

    private static FDate newDate(final int minutesAfterNoon) {
        return FDateBuilder.newDate(2020, 1, 1).addHours(12).addMinutes(minutesAfterNoon);
    }

    private static List<FDate> newValues(final int fromMinute, final int toMinute) {
        final List<FDate> values = new ArrayList<>();
        for (int minute = fromMinute; minute <= toMinute; minute++) {
            values.add(newDate(minute));
        }
        return values;
    }

    private static List<FDate> readValues(final ATimeSeriesDB<String, FDate> table, final String key) {
        final List<FDate> values = new ArrayList<>();
        try (ICloseableIterator<FDate> it = table.rangeValues(key, FDates.MIN_DATE, FDates.MAX_DATE).iterator()) {
            Lists.toListWithoutHasNext(it, values);
        }
        return values;
    }

    private ATimeSeriesDB<String, FDate> newTable(final String name) {
        return new ATimeSeriesDB<String, FDate>(name) {

            @Override
            protected ISerde<FDate> newValueSerde() {
                return FDateSerde.GET;
            }

            @Override
            protected Integer newValueFixedLength() {
                return FDateSerde.FIXED_LENGTH;
            }

            @Override
            protected int newBatchFlushInterval() {
                return BATCH_FLUSH_INTERVAL;
            }

            @Override
            public ITimeSeriesBaseDirectory getBaseDirectory() {
                return new TimeSeriesBaseDirectory(baseDirectory);
            }

            @Override
            public String innerHashKeyToString(final String key) {
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
        };
    }

    private ATimeSeriesUpdater<String, FDate> newUpdater(final ATimeSeriesDB<String, FDate> table,
            final List<FDate> values) {
        return new ATimeSeriesUpdater<String, FDate>(KEY, table) {

            @Override
            protected ICloseableIterable<? extends FDate> getSource(final FDate updateFrom) {
                return WrapperCloseableIterable.maybeWrap(values);
            }

            @Override
            protected void onUpdateStarted(final FDate updateStart) {}

            @Override
            protected void onUpdateFinished() {}

            @Override
            protected FDate extractStartTime(final FDate element) {
                return element;
            }

            @Override
            protected FDate extractEndTime(final FDate element) {
                return element;
            }

            @Override
            protected void onElement(final ITimeSeriesUpdateProgress relativeProgress, final long relativeCount) {}

            @Override
            protected void onFlush(final ITimeSeriesUpdateProgress relativeProgress, final long flushIndex) {}

            @Override
            public Percent getProgress(final FDate minTime, final FDate maxTime) {
                return null;
            }
        };
    }

}
