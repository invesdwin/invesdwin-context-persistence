package de.invesdwin.context.persistence.timeseries;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Ignore;
import org.junit.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseries.ezdb.ADelegateRangeTable;
import de.invesdwin.context.persistence.timeseries.serde.FDateSerde;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.ATimeSeriesUpdater;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.IncompleteUpdateFoundException;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.lang.ProcessedEventsRateString;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.math.decimal.scaled.PercentScale;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FDates;
import de.invesdwin.util.time.fdate.FTimeUnit;
import ezdb.batch.RangeBatch;
import ezdb.serde.Serde;

@NotThreadSafe
@Ignore("manual test")
public class DatabasePerformanceTest extends ATest {

    //    static {
    //makes it sloooooow
    //        LZ4Streams.setAllowJniCompressor(true);
    //    }

    private static final int READS = 10;
    private static final int VALUES = 1_000_000;
    private static final String HASH_KEY = "HASH_KEY";
    private static final int FLUSH_INTERVAL = ATimeSeriesUpdater.BATCH_FLUSH_INTERVAL;

    @Test
    public void testLevelDbPerformance() {
        final ADelegateRangeTable<String, FDate, FDate> table = new ADelegateRangeTable<String, FDate, FDate>(
                "testLevelDbPerformance") {
            @Override
            protected File getDirectory() {
                return new File(ContextProperties.getCacheDirectory(), ADelegateRangeTable.class.getSimpleName());
            }

            @Override
            protected Serde<FDate> newValueSerde() {
                return FDateSerde.GET;
            }

            @Override
            protected Serde<FDate> newRangeKeySerde() {
                return FDateSerde.GET;
            }
        };

        RangeBatch<String, FDate, FDate> batch = table.newRangeBatch();
        final Instant writesStart = new Instant();
        int i = 0;
        for (final FDate date : newValues()) {
            batch.put(HASH_KEY, date, date);
            i++;
            if (i % FLUSH_INTERVAL == 0) {
                printProgress("Writes", writesStart, i, VALUES);
                try {
                    batch.flush();
                    batch.close();
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
                batch = table.newRangeBatch();
            }
        }
        try {
            batch.flush();
            batch.close();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        printProgress("WritesFinished", writesStart, VALUES, VALUES);

        final Instant readsStart = new Instant();
        for (int reads = 1; reads <= READS; reads++) {
            FDate prevValue = null;
            final ICloseableIterator<FDate> range = table.rangeValues(HASH_KEY);
            int count = 0;
            while (true) {
                try {
                    final FDate value = range.next();
                    if (prevValue != null) {
                        Assertions.checkTrue(prevValue.isBefore(value));
                    }
                    prevValue = value;
                    count++;
                } catch (final NoSuchElementException e) {
                    break;
                }
            }
            Assertions.checkEquals(count, VALUES);
            printProgress("Reads", readsStart, VALUES * reads, VALUES * READS);
        }
        printProgress("ReadsFinished", readsStart, VALUES * READS, VALUES * READS);
    }

    private void printProgress(final String action, final Instant start, final long count, final int maxCount) {
        final Duration duration = start.toDuration();
        log.info("%s: %s/%s (%s) %s during %s", action, count, maxCount,
                new Percent(count, maxCount).toString(PercentScale.PERCENT),
                new ProcessedEventsRateString(count, duration), duration);
    }

    private ICloseableIterable<FDate> newValues() {
        return FDates.iterable(FDate.MIN_DATE, FDate.MIN_DATE.addMilliseconds(VALUES - 1), FTimeUnit.MILLISECONDS, 1);
    }

    @Test
    public void testTimeSeriesDbPerformance() throws IncompleteUpdateFoundException {
        final ATimeSeriesDB<String, FDate> table = new ATimeSeriesDB<String, FDate>("testTimeSeriesDbPerformance") {

            @Override
            protected File getBaseDirectory() {
                return new File(ContextProperties.getCacheDirectory(), ATimeSeriesDB.class.getSimpleName());
            }

            @Override
            protected Serde<FDate> newValueSerde() {
                return FDateSerde.GET;
            }

            @Override
            protected Integer newFixedLength() {
                return FDateSerde.FIXED_LENGTH;
            }

            @Override
            protected String hashKeyToString(final String key) {
                return "testTimeSeriesDbPerformance_" + key;
            }

            @Override
            protected FDate extractTime(final FDate value) {
                return value;
            }
        };

        final Instant writesStart = new Instant();
        final ATimeSeriesUpdater<String, FDate> updater = new ATimeSeriesUpdater<String, FDate>(HASH_KEY, table) {

            @Override
            protected ICloseableIterable<FDate> getSource(final FDate updateFrom) {
                return newValues();
            }

            @Override
            protected void onUpdateFinished(final Instant updateStart) {
                printProgress("WritesFinished", writesStart, VALUES, VALUES);
            }

            @Override
            protected void onUpdateStart() {}

            @Override
            protected FDate extractTime(final FDate element) {
                return element;
            }

            @Override
            protected FDate extractEndTime(final FDate element) {
                return element;
            }

            @Override
            protected void onFlush(final int flushIndex, final Instant flushStart,
                    final ATimeSeriesUpdater<String, FDate>.UpdateProgress updateProgress) {
                printProgress("Writes", writesStart, updateProgress.getCount() * flushIndex, VALUES);
            }
        };
        Assertions.checkTrue(updater.update());

        final Instant readsStart = new Instant();
        for (int reads = 1; reads <= READS; reads++) {
            FDate prevValue = null;
            final ICloseableIterator<? extends FDate> range = table.rangeValues(HASH_KEY, null, null).iterator();
            int count = 0;
            while (true) {
                try {
                    final FDate value = range.next();
                    if (prevValue != null) {
                        Assertions.checkTrue(prevValue.isBefore(value));
                    }
                    prevValue = value;
                    count++;
                } catch (final NoSuchElementException e) {
                    break;
                }
            }
            Assertions.checkEquals(count, VALUES);
            printProgress("Reads", readsStart, VALUES * reads, VALUES * READS);
        }
        printProgress("ReadsFinished", readsStart, VALUES * READS, VALUES * READS);
    }

}
