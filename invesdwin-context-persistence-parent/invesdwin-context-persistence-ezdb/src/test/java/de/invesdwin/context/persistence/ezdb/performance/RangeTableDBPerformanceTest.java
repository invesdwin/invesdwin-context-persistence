package de.invesdwin.context.persistence.ezdb.performance;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.ezdb.table.range.ADelegateRangeTable;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;
import ezdb.table.range.RangeBatch;

@NotThreadSafe
@Disabled("manual test")
public class RangeTableDBPerformanceTest extends ADatabasePerformanceTest {

    @Test
    public void testLevelDbPerformance() throws InterruptedException {
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

            //            @Override
            //            protected IRangeTableDb newDiskDb() {
            //                return new LsmTreeRangeTableDb(internalMethods);
            //            }

        };

        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        RangeBatch<String, FDate, FDate> batch = table.newRangeBatch();
        final Instant writesStart = new Instant();
        int i = 0;
        for (final FDate date : newValues()) {
            batch.put(HASH_KEY, date, date);
            i++;
            if (i % FLUSH_INTERVAL == 0) {
                try {
                    if (loopCheck.check()) {
                        printProgress("Writes", writesStart, i, VALUES);
                    }
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
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

        readIterator(table);
        readGet(table);
        readGetLatest(table);
    }

    private void readIterator(final ADelegateRangeTable<String, FDate, FDate> table) throws InterruptedException {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
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
            if (loopCheck.check()) {
                printProgress("Reads", readsStart, VALUES * reads, VALUES * READS);
            }
        }
        printProgress("ReadsFinished", readsStart, VALUES * READS, VALUES * READS);
    }

    private void readGet(final ADelegateRangeTable<String, FDate, FDate> table) throws InterruptedException {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final List<FDate> values = Lists.toList(newValues());
        final Instant readsStart = new Instant();
        for (int reads = 1; reads <= READS; reads++) {
            FDate prevValue = null;
            for (int i = 0; i < values.size(); i++) {
                try {
                    final FDate value = table.get(HASH_KEY, values.get(i));
                    if (prevValue != null) {
                        Assertions.checkTrue(prevValue.isBefore(value));
                    }
                    prevValue = value;
                } catch (final NoSuchElementException e) {
                    break;
                }
            }
            if (loopCheck.check()) {
                printProgress("Gets", readsStart, VALUES * reads, VALUES * READS);
            }
        }
        printProgress("GetsFinished", readsStart, VALUES * READS, VALUES * READS);
    }

    private void readGetLatest(final ADelegateRangeTable<String, FDate, FDate> table) throws InterruptedException {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final List<FDate> values = Lists.toList(newValues());
        final Instant readsStart = new Instant();
        for (int reads = 1; reads <= READS; reads++) {
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
                printProgress("GetLatests", readsStart, VALUES * reads, VALUES * READS);
            }
        }
        printProgress("GetLatestsFinished", readsStart, VALUES * READS, VALUES * READS);
    }

}
