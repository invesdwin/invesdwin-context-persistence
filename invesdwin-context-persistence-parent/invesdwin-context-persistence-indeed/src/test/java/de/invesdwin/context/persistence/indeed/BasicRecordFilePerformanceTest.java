package de.invesdwin.context.persistence.indeed;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import com.indeed.lsmtree.recordlog.BasicRecordFile;
import com.indeed.lsmtree.recordlog.RecordFile;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.marshallers.serde.basic.TimedDecimalSerde;
import de.invesdwin.util.math.decimal.TimedDecimal;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
//@Ignore("manual test")
public class BasicRecordFilePerformanceTest extends ADatabasePerformanceTest {

    @Test
    public void testBasicRecordFilePerformance() throws IOException, InterruptedException {
        final File directory = new File(ContextProperties.getCacheDirectory(),
                BasicRecordFilePerformanceTest.class.getSimpleName());
        Files.deleteNative(directory);
        Files.forceMkdir(directory);
        final File file = new File(directory, "testBasicRecordFilePerformance");

        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final Instant writesStart = new Instant();
        try (BasicRecordFile.Writer<TimedDecimal> writer = new BasicRecordFile.Writer<>(file,
                IndeedSerializer.valueOf(TimedDecimalSerde.GET))) {
            int i = 0;
            for (final FDate date : newValues()) {
                writer.append(new TimedDecimal(date, date.millisValue()));
                i++;
                if (i % FLUSH_INTERVAL == 0) {
                    if (loopCheck.check()) {
                        printProgress("Writes", writesStart, i, VALUES);
                    }
                }
            }
            printProgress("WritesFinished", writesStart, VALUES, VALUES);
        }

        readIterator(file);

    }

    private void readIterator(final File file) throws IOException, InterruptedException {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck();
        final Instant readsStart = new Instant();
        for (int reads = 1; reads <= READS; reads++) {
            FDate prevValue = null;
            int count = 0;
            try (BasicRecordFile<TimedDecimal> reader = new BasicRecordFile<>(file,
                    IndeedSerializer.valueOf(TimedDecimalSerde.GET))) {
                final RecordFile.Reader<TimedDecimal> iterator = reader.reader();
                while (iterator.next()) {
                    final FDate value = iterator.get().getTime();
                    if (prevValue != null) {
                        Assertions.checkTrue(prevValue.isBefore(value));
                    }
                    prevValue = value;
                    count++;
                }
                Assertions.checkEquals(count, VALUES);
                if (loopCheck.check()) {
                    printProgress("Reads", readsStart, VALUES * reads, VALUES * READS);
                }
            }
        }
        printProgress("ReadsFinished", readsStart, VALUES * READS, VALUES * READS);
    }

}
