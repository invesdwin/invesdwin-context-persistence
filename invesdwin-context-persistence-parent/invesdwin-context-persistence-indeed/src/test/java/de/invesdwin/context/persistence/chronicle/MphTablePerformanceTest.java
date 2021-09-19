package de.invesdwin.context.persistence.chronicle;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import com.indeed.mph.TableConfig;
import com.indeed.mph.TableReader;
import com.indeed.mph.TableWriter;
import com.indeed.mph.serializers.SmartLongSerializer;
import com.indeed.mph.serializers.SmartVLongSerializer;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ATransformingIterable;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
//@Ignore("manual test")
public class MphTablePerformanceTest extends ADatabasePerformanceTest {

    @Test
    public void testMphTablePerformance() throws IOException {
        final File directory = new File(ContextProperties.getCacheDirectory(),
                MphTablePerformanceTest.class.getSimpleName());
        Files.deleteNative(directory);
        Files.forceMkdir(directory);
        final File file = new File(directory, "testMphTablePerformance");

        final Instant writesStart = new Instant();
        final TableConfig<Long, Long> config = new TableConfig<Long, Long>()
                .withKeySerializer(new SmartLongSerializer())
                .withValueSerializer(new SmartVLongSerializer());
        final ATransformingIterable<FDate, com.indeed.util.core.Pair<Long, Long>> entries = new ATransformingIterable<FDate, com.indeed.util.core.Pair<Long, Long>>(
                newValues()) {
            @Override
            protected com.indeed.util.core.Pair<Long, Long> transform(final FDate value) {
                return com.indeed.util.core.Pair.of(value.millisValue(), value.millisValue());
            }
        };
        //can not append to an existing table
        TableWriter.write(file, config, entries);
        printProgress("WritesFinished", writesStart, VALUES, VALUES);

        readIterator(file);
        readGet(file);
    }

    private void readIterator(final File file) throws IOException {
        final Instant readsStart = new Instant();
        for (int reads = 1; reads <= READS; reads++) {
            FDate prevValue = null;
            int count = 0;
            try (TableReader<Long, Long> reader = TableReader.open(file)) {
                final Iterator<com.indeed.util.core.Pair<Long, Long>> iterator = reader.iterator();
                while (iterator.hasNext()) {
                    final FDate value = new FDate(iterator.next().getSecond());
                    //                        if (prevValue != null) {
                    //                            Assertions.checkTrue(prevValue.isBefore(value));
                    //                        }
                    prevValue = value;
                    count++;
                }
                Assertions.checkEquals(count, VALUES);
                printProgress("Reads", readsStart, VALUES * reads, VALUES * READS);
            }
        }
        printProgress("ReadsFinished", readsStart, VALUES * READS, VALUES * READS);
    }

    private void readGet(final File file) throws IOException {
        final List<FDate> values = Lists.toList(newValues());
        final Instant readsStart = new Instant();
        for (int reads = 1; reads <= READS; reads++) {
            FDate prevValue = null;
            try (TableReader<Long, Long> reader = TableReader.open(file)) {
                for (int i = 0; i < values.size(); i++) {
                    final FDate value = new FDate(reader.get(values.get(i).millisValue()));
                    if (prevValue != null) {
                        Assertions.checkTrue(prevValue.isBefore(value));
                    }
                    prevValue = value;
                }
            }
            printProgress("Gets", readsStart, VALUES * reads, VALUES * READS);
        }
        printProgress("GetsFinished", readsStart, VALUES * READS, VALUES * READS);
    }

}
