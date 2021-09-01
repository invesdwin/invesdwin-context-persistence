package de.invesdwin.context.persistence.timeseries;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseries.ezdb.ADelegateRangeTable;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.IncompleteUpdateFoundException;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.lang.ProcessedEventsRateString;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.math.decimal.scaled.PercentScale;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;
import ezdb.batch.RangeBatch;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;

@NotThreadSafe
//@Ignore("manual test")
public class DatabasePerformanceTest extends ATest {

    //    static {
    //makes it sloooooow
    //        LZ4Streams.setAllowJniCompressor(true);
    //    }

    private static final int READS = 10;
    private static final int VALUES = 100_000_000;
    private static final String HASH_KEY = "HASH_KEY";
    private static final int FLUSH_INTERVAL = ATimeSeriesUpdater.BATCH_FLUSH_INTERVAL;

    @Test
    public void testLevelDbPerformance() {
        final ADelegateRangeTable<String, FDate, FDate> table = new ADelegateRangeTable<String, FDate, FDate>(
                "testLevelDbPerformance") {
            @Override
            protected File getBaseDirectory() {
                return ContextProperties.TEMP_DIRECTORY;
            }

            @Override
            protected ISerde<FDate> newValueSerde() {
                return FDateSerde.GET;
            }

            @Override
            protected ISerde<FDate> newRangeKeySerde() {
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
    public void testChronicleQueuePerformance() {
        final ChronicleQueue queue = SingleChronicleQueueBuilder
                .binary(new File(ContextProperties.TEMP_CLASSPATH_DIRECTORY, "testLevelDbPerformance"))
                .build();

        final Instant writesStart = new Instant();
        try (ExcerptAppender acquireAppender = queue.acquireAppender()) {
            int i = 0;
            for (final FDate date : newValues()) {
                try (DocumentContext doc = acquireAppender.writingDocument()) {
                    doc.wire().bytes().writeLong(date.millisValue());
                }
                i++;
                if (i % FLUSH_INTERVAL == 0) {
                    printProgress("Writes", writesStart, i, VALUES);
                }
            }
        }
        printProgress("WritesFinished", writesStart, VALUES, VALUES);

        final Instant readsStart = new Instant();
        for (int reads = 1; reads <= READS; reads++) {
            FDate prevValue = null;
            int count = 0;
            try (ExcerptTailer tailer = queue.createTailer()) {
                final net.openhft.chronicle.bytes.Bytes<java.nio.ByteBuffer> bytes = net.openhft.chronicle.bytes.Bytes
                        .elasticByteBuffer();
                while (tailer.readBytes(bytes)) {
                    final FDate value = FDate.valueOf(bytes.readLong());
                    if (prevValue != null) {
                        Assertions.checkTrue(prevValue.isBefore(value));
                    }
                    prevValue = value;
                    count++;
                }
                Assertions.checkEquals(count, VALUES);
                printProgress("Reads", readsStart, VALUES * reads, VALUES * READS);
            }
        }
        printProgress("ReadsFinished", readsStart, VALUES * READS, VALUES * READS);
    }

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

            //            @Override
            //            protected ICompressorFactory newCompressorFactory() {
            //                return FastLZ4CompressorFactory.INSTANCE;
            //            }

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
