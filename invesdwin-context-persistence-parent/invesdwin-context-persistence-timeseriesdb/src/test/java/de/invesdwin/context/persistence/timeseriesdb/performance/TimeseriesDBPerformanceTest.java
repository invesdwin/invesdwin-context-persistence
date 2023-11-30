package de.invesdwin.context.persistence.timeseriesdb.performance;

import java.io.File;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.IPersistentMapType;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.PersistentMapType;
import de.invesdwin.context.persistence.timeseriesdb.storage.TimeSeriesStorage;
import de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.IUpdateProgress;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
@Disabled("manual test")
public class TimeseriesDBPerformanceTest extends ADatabasePerformanceTest {

    @Test
    public void testTimeSeriesDbPerformance() throws IncompleteUpdateRetryableException, InterruptedException {
        final ATimeSeriesDB<String, FDate> table = new ATimeSeriesDB<String, FDate>("testTimeSeriesDbPerformance") {

            @Override
            protected TimeSeriesStorage newStorage(final File directory, final Integer valueFixedLength,
                    final ICompressionFactory compressionFactory) {
                return new TimeSeriesStorage(directory, valueFixedLength, compressionFactory) {
                    @Override
                    protected IPersistentMapType getMapType() {
                        return PersistentMapType.DISK_FAST;
                    }
                };
            }

            @Override
            public File getBaseDirectory() {
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
            public FDate extractStartTime(final FDate value) {
                return value;
            }

            @Override
            public FDate extractEndTime(final FDate value) {
                return value;
            }

            //            @Override
            //            protected ICompressionFactory newCompressionFactory() {
            //                return FastLZ4CompressionFactory.INSTANCE;
            //            }

        };

        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
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
            protected FDate extractStartTime(final FDate element) {
                return element;
            }

            @Override
            protected FDate extractEndTime(final FDate element) {
                return element;
            }

            @Override
            protected void onFlush(final int flushIndex, final IUpdateProgress<String, FDate> updateProgress) {
                try {
                    if (loopCheck.check()) {
                        printProgress("Writes", writesStart, updateProgress.getValueCount() * flushIndex, VALUES);
                    }
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Percent getProgress() {
                return null;
            }
        };
        Assertions.checkTrue(updater.update());

        readIterator(table, "Cold", 1);
        readIterator(table, "Warm", READS);
        readGetLatest(table, "Cold", 1);
        readGetLatest(table, "Warm", READS);
    }

    private void readIterator(final ATimeSeriesDB<String, FDate> table, final String suffix, final long maxReads)
            throws InterruptedException {
        final Instant readsStart = new Instant();
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        for (long reads = 1; reads <= maxReads; reads++) {
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
            Assertions.checkEquals(VALUES, count);
            if (loopCheck.check()) {
                printProgress("Reads" + suffix, readsStart, VALUES * reads, VALUES * maxReads);
            }
        }
        printProgress("ReadsFinished" + suffix, readsStart, VALUES * maxReads, VALUES * maxReads);
    }

    private void readGetLatest(final ATimeSeriesDB<String, FDate> table, final String suffix, final long countReads)
            throws InterruptedException {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final List<FDate> values = Lists.toList(newValues());
        final Instant readsStart = new Instant();
        for (long reads = 1; reads <= countReads; reads++) {
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
            if (loopCheck.check()) {
                printProgress("GetLatests" + suffix, readsStart, VALUES * reads, VALUES * countReads);
            }
        }
        printProgress("GetLatests" + suffix + "Finished", readsStart, VALUES * countReads, VALUES * countReads);
    }

}
