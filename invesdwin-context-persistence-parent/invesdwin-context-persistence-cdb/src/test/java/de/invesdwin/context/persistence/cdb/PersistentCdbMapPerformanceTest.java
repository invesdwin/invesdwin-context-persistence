package de.invesdwin.context.persistence.cdb;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.strangegizmo.cdb.CdbMake;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.persistentmap.APersistentMap;
import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.streams.closeable.Closeables;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
@Disabled("manual test")
public class PersistentCdbMapPerformanceTest extends ADatabasePerformanceTest {

    @Test
    public void testCdbMapPerformance() throws InterruptedException, IOException {
        final String name = "testCdbMapPerformance.cdb";
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final Instant writesStart = new Instant();
        int i = 0;
        final CdbMake delegate = new CdbMake();
        final File file = new File(ContextProperties.TEMP_DIRECTORY, APersistentMap.class.getSimpleName() + "/"
                + PersistentCdbMapFactory.class.getSimpleName() + "/" + name);
        Files.forceMkdirParent(file);
        delegate.start(file.getAbsolutePath());
        for (final FDate date : newValues()) {
            delegate.add(FDateSerde.GET.toBytes(date), FDateSerde.GET.toBytes(date));
            i++;
            if (i % FLUSH_INTERVAL == 0) {
                if (loopCheck.check()) {
                    printProgress("Writes", writesStart, i, VALUES);
                }
            }
        }
        delegate.finish();
        printProgress("WritesFinished", writesStart, VALUES, VALUES);

        @SuppressWarnings("resource")
        final APersistentMap<FDate, FDate> table = new APersistentMap<FDate, FDate>(name) {
            @Override
            public File getBaseDirectory() {
                return ContextProperties.TEMP_DIRECTORY;
            }

            @Override
            public ISerde<FDate> newKeySerde() {
                return FDateSerde.GET;
            }

            @Override
            public ISerde<FDate> newValueSerde() {
                return FDateSerde.GET;
            }

            @Override
            protected IPersistentMapFactory<FDate, FDate> newFactory() {
                return new PersistentCdbMapFactory<FDate, FDate>();
            }
        };

        readIterator(table);
        readGet(table);
        table.deleteTable();
    }

    private void readIterator(final APersistentMap<FDate, FDate> table) {
        final Instant readsStart = new Instant();
        for (int reads = 1; reads <= READS; reads++) {
            //            FDate prevValue = null;
            final Iterator<FDate> range = table.values().iterator();
            int count = 0;
            while (true) {
                try {
                    final FDate value = range.next();
                    //                    if (prevValue != null) {
                    //                        Assertions.checkTrue(prevValue.isBefore(value));
                    //                    }
                    //                    prevValue = value;
                    count++;
                } catch (final NoSuchElementException e) {
                    break;
                }
            }
            Closeables.close(range);
            Assertions.checkEquals(count, VALUES);
            printProgress("Reads", readsStart, VALUES * reads, VALUES * READS);
        }
        printProgress("ReadsFinished", readsStart, VALUES * READS, VALUES * READS);
    }

    private void readGet(final APersistentMap<FDate, FDate> table) {
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
            printProgress("Gets", readsStart, VALUES * reads, VALUES * READS);
        }
        printProgress("GetsFinished", readsStart, VALUES * READS, VALUES * READS);
    }

}
