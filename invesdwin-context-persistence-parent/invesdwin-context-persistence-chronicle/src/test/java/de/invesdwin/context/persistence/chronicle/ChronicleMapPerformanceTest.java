package de.invesdwin.context.persistence.chronicle;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

@NotThreadSafe
//@Ignore("manual test")
public class ChronicleMapPerformanceTest extends ADatabasePerformanceTest {

    @Test
    public void testChronicleMapPerformance() throws IOException, InterruptedException {
        final File directory = new File(ContextProperties.getCacheDirectory(),
                ChronicleMapPerformanceTest.class.getSimpleName());
        Files.deleteNative(directory);
        Files.forceMkdir(directory);

        final Instant writesStart = new Instant();
        final ChronicleMapBuilder<Long, Long> mapBuilder = ChronicleMapBuilder.of(Long.class, Long.class)
                .name("testChronicleMapPerformance")
                .constantKeySizeBySample(FDate.MAX_DATE.millisValue())
                .constantValueSizeBySample(FDate.MAX_DATE.millisValue())
                //                .maxBloatFactor(1_000)
                //                .entries(10_000_000);
                .entries(VALUES);
        //        final ChronicleMap<Long, Long> map = mapBuilder.create();
        final ChronicleMap<Long, Long> map = mapBuilder
                .createPersistedTo(new File(directory, "testChronicleMapPerformance"));
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        int i = 0;
        for (final FDate date : newValues()) {
            map.put(date.millisValue(), date.millisValue());
            i++;
            if (i % FLUSH_INTERVAL == 0) {
                if (loopCheck.check()) {
                    printProgress("Writes", writesStart, i, VALUES);
                }
            }
        }
        printProgress("WritesFinished", writesStart, VALUES, VALUES);

        readIterator(map);
        readGet(map);
    }

    private void readIterator(final ChronicleMap<Long, Long> table) throws InterruptedException {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final Instant readsStart = new Instant();
        for (int reads = 1; reads <= READS; reads++) {
            //            FDate prevValue = null;
            final Iterator<Long> range = table.values().iterator();
            int count = 0;
            while (true) {
                try {
                    final FDate value = new FDate(range.next());
                    //                    if (prevValue != null) {
                    //                        Assertions.checkTrue(prevValue.isBefore(value));
                    //                    }
                    //                    prevValue = value;
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

    private void readGet(final ChronicleMap<Long, Long> table) throws InterruptedException {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final List<FDate> values = Lists.toList(newValues());
        final Instant readsStart = new Instant();
        for (int reads = 1; reads <= READS; reads++) {
            FDate prevValue = null;
            for (int i = 0; i < values.size(); i++) {
                try {
                    final FDate value = new FDate(table.get(values.get(i).millisValue()));
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

}
