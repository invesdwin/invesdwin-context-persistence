package de.invesdwin.context.persistence.timeseries.performance;

import java.io.File;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Ignore;
import org.junit.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.streams.compressor.DisabledCompressionFactory;
import de.invesdwin.context.integration.streams.compressor.ICompressionFactory;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.IncompleteUpdateFoundException;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
@Ignore("manual test")
public class TimeseriesDBPerformanceTest extends ADatabasePerformanceTest {

    @Test
    public void testTimeSeriesDbPerformance() throws IncompleteUpdateFoundException {
        final ATimeSeriesDB<String, FDate> table = new ATimeSeriesDB<String, FDate>("testTimeSeriesDbPerformance") {

            @Override
            protected File getBaseDirectory() {
                return ContextProperties.TEMP_DIRECTORY;
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
                return "testTimeSeriesDbPerformance_" + key;
            }

            @Override
            protected FDate extractEndTime(final FDate value) {
                return value;
            }

            @Override
            protected ICompressionFactory newCompressionFactory() {
                return DisabledCompressionFactory.INSTANCE;
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
            protected void onUpdateStart() {
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

            @Override
            public Percent getProgress() {
                return null;
            }
        };
        Assertions.checkTrue(updater.update());

        readIterator(table);
        readGetLatest(table, "Cold", 1);
        readGetLatest(table, "Warm", READS);
    }

    private void readIterator(final ATimeSeriesDB<String, FDate> table) {
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

    private void readGetLatest(final ATimeSeriesDB<String, FDate> table, final String suffix, final int countReads) {
        final List<FDate> values = Lists.toList(newValues());
        final Instant readsStart = new Instant();
        for (int reads = 1; reads <= countReads; reads++) {
            FDate prevValue = null;
            for (int i = 0; i < values.size(); i++) {
                try {
                    final FDate value = table.getLatestValue(HASH_KEY, values.get(i));
                    if (prevValue != null) {
                        Assertions.checkTrue(prevValue.isBefore(value));
                    }
                    prevValue = value;
                } catch (final NoSuchElementException e) {
                    break;
                }
            }
            printProgress("Gets" + suffix, readsStart, VALUES * reads, VALUES * countReads);
        }
        printProgress("Gets" + suffix + "Finished", readsStart, VALUES * countReads, VALUES * countReads);
    }

}
