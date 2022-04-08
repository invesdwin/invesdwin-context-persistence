package de.invesdwin.context.persistence.leveldb;

import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.persistence.leveldb.serde.FDateSerde;
import de.invesdwin.context.persistence.leveldb.timeseries.ATimeSeriesDB;
import de.invesdwin.context.persistence.leveldb.timeseries.ATimeSeriesUpdater;
import de.invesdwin.context.persistence.leveldb.timeseries.IncompleteUpdateFoundException;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.math.decimal.Decimal;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.math.decimal.scaled.PercentScale;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.date.FTimeUnit;
import ezdb.serde.Serde;

@NotThreadSafe
public class DatabasePerformanceTest extends ATest {

    private static final int READS = 10;
    private static final int VALUES = 100_000_000;
    private static final String HASH_KEY = "HASH_KEY";
    private static final int FLUSH_INTERVAL = ATimeSeriesUpdater.BATCH_FLUSH_INTERVAL;

    private void printProgress(final String action, final Instant start, final int count, final int maxCount) {
        final long milliseconds = start.toDuration().longValue(FTimeUnit.MILLISECONDS);
        log.info("%s: %s/%s (%s) %s/ms in %s ms", action, count, maxCount,
                new Percent(count, maxCount).toString(PercentScale.PERCENT),
                Decimal.valueOf(count).divide(milliseconds).round(2), milliseconds);
    }

    private ICloseableIterable<FDate> newValues() {
        return FDates.iterable(FDate.MIN_DATE, FDate.MIN_DATE.addMilliseconds(VALUES - 1), FTimeUnit.MILLISECONDS, 1);
    }

    @Test
    public void testTimeSeriesDbPerformance() throws IncompleteUpdateFoundException {
        final ATimeSeriesDB<String, FDate> table = new ATimeSeriesDB<String, FDate>("testTimeSeriesDbPerformance") {

            @Override
            protected Serde<FDate> newValueSerde() {
                return FDateSerde.get;
            }

            @Override
            protected Integer newFixedLength() {
                return FDateSerde.get.toBytes(FDate.MAX_DATE).length;
            }

            @Override
            protected String getDatabaseName(final String key) {
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
            protected ICloseableIterable<FDate> getSource() {
                return newValues();
            }

            @Override
            protected void onUpdateFinished(final Instant updateStart) {
                printProgress("WritesFinished", writesStart, VALUES, VALUES);
            }

            @Override
            protected void onUpdateStart() {
            }

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
            try (ICloseableIterator<? extends FDate> range = table.rangeValues(HASH_KEY, null, null)) {
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
        }
        printProgress("ReadsFinished", readsStart, VALUES * READS, VALUES * READS);
    }

}
