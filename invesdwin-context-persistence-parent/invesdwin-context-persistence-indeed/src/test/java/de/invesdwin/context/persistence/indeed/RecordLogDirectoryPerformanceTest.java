package de.invesdwin.context.persistence.indeed;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import com.indeed.lsmtree.recordlog.RecordFile;
import com.indeed.lsmtree.recordlog.RecordLogDirectory;
import com.indeed.util.compress.SnappyCodec;

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
@Ignore("manual test")
public class RecordLogDirectoryPerformanceTest extends ADatabasePerformanceTest {

    @Test
    public void testRecordLogDirectoryPerformance() throws IOException, InterruptedException {
        final File directory = new File(ContextProperties.getCacheDirectory(),
                RecordLogDirectoryPerformanceTest.class.getSimpleName());
        Files.deleteNative(directory);
        Files.forceMkdir(directory);
        final File file = new File(directory, "testRecordLogDirectoryPerformance");

        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final Instant writesStart = new Instant();
        try (RecordLogDirectory.Writer<TimedDecimal> writer = RecordLogDirectory.Writer.create(file,
                IndeedSerializer.valueOf(TimedDecimalSerde.GET), new SnappyCodec(), Integer.MAX_VALUE)) {
            int i = 0;
            for (final FDate date : newValues()) {
                writer.append(new TimedDecimal(date, date.millisValue()));
                i++;
                if (i % FLUSH_INTERVAL == 0) {
                    if (loopCheck.check()) {
                        printProgress("Writes", writesStart, i, VALUES);
                    }
                    //                    writer.roll();
                }
            }
            writer.roll();
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
            try (RecordLogDirectory<TimedDecimal> reader = new RecordLogDirectory.Builder<TimedDecimal>(file,
                    IndeedSerializer.valueOf(TimedDecimalSerde.GET), new SnappyCodec()).build()) {
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
