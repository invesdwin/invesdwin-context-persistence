package de.invesdwin.context.persistence.indeed;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import com.indeed.lsmtree.core.StorageType;
import com.indeed.lsmtree.core.Store;
import com.indeed.lsmtree.core.StoreBuilder;
import com.indeed.util.compress.SnappyCodec;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
//@Ignore("manual test")
public class LsmTreePerformanceTest extends ADatabasePerformanceTest {

    @Test
    public void testLsmTreePerformance() throws IOException, InterruptedException {
        final File directory = new File(ContextProperties.getCacheDirectory(),
                LsmTreePerformanceTest.class.getSimpleName());
        Files.deleteNative(directory);
        Files.forceMkdir(directory);
        final File file = new File(directory, "testLsmTreePerformance");

        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final Instant writesStart = new Instant();
        final Store<FDate, FDate> store = new StoreBuilder<FDate, FDate>(file, IndeedSerializer.valueOf(FDateSerde.GET),
                IndeedSerializer.valueOf(FDateSerde.GET)).setStorageType(StorageType.BLOCK_COMPRESSED)
                        .setCodec(new SnappyCodec())
                        .build();
        int i = 0;
        for (final FDate date : newValues()) {
            store.put(date, date);
            i++;
            if (i % FLUSH_INTERVAL == 0) {
                if (loopCheck.check()) {
                    printProgress("Writes", writesStart, i, VALUES);
                }
            }
        }
        //        store.waitForCompactions();
        printProgress("WritesFinished", writesStart, VALUES, VALUES);

        readIterator(store);
        readGet(store);
        readGetLatest(store);
        store.close();
    }

    private void readIterator(final Store<FDate, FDate> table) throws InterruptedException, IOException {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final Instant readsStart = new Instant();
        for (int reads = 1; reads <= READS; reads++) {
            FDate prevValue = null;
            final Iterator<Store.Entry<FDate, FDate>> range = table.iterator();
            int count = 0;
            while (true) {
                try {
                    final FDate value = range.next().getValue();
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

    private void readGet(final Store<FDate, FDate> table) throws InterruptedException, IOException {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final List<FDate> values = Lists.toList(newValues());
        final Instant readsStart = new Instant();
        for (int reads = 1; reads <= READS; reads++) {
            FDate prevValue = null;
            for (int i = 0; i < values.size(); i++) {
                try {
                    final FDate value = table.get(values.get(i));
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

    private void readGetLatest(final Store<FDate, FDate> table) throws InterruptedException, IOException {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final List<FDate> values = Lists.toList(newValues());
        final Instant readsStart = new Instant();
        for (int reads = 1; reads <= READS; reads++) {
            FDate prevValue = null;
            for (int i = 0; i < values.size(); i++) {
                try {
                    final Store.Entry<FDate, FDate> entry = table.floor(values.get(i));
                    final FDate value = entry.getValue();
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
