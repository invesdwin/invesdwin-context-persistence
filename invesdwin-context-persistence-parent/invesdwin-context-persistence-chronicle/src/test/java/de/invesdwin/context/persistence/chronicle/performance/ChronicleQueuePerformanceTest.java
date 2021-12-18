package de.invesdwin.context.persistence.chronicle.performance;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;

@NotThreadSafe
@Disabled("manual test")
public class ChronicleQueuePerformanceTest extends ADatabasePerformanceTest {

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

}
