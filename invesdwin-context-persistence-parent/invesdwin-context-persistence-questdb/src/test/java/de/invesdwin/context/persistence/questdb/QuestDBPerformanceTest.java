package de.invesdwin.context.persistence.questdb;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;

/**
 * https://questdb.io/docs/reference/api/java-embedded/
 */
@NotThreadSafe
//@Ignore("manual test")
public class QuestDBPerformanceTest extends ADatabasePerformanceTest {

    @Test
    public void testQuestDbPerformance() throws InterruptedException, SqlException {
        final CairoConfiguration configuration = new DefaultCairoConfiguration(
                ContextProperties.getCacheDirectory().getAbsolutePath());
        final Instant writesStart = new Instant();
        int i = 0;
        final CairoEngine engine = new CairoEngine(configuration);
        final SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1);
        try (SqlCompiler compiler = new SqlCompiler(engine)) {

            compiler.compile("create table keyValue (value long, key timestamp) timestamp(key)", ctx);

            try (TableWriter writer = engine.getWriter(ctx.getCairoSecurityContext(), "keyValue", "insert")) {
                for (final FDate date : newValues()) {
                    final TableWriter.Row row = writer.newRow(date.millisValue());
                    row.putLong(0, date.millisValue());
                    row.append();
                    i++;
                    if (i % FLUSH_INTERVAL == 0) {
                        printProgress("Writes", writesStart, i, VALUES);
                        writer.commit();
                    }
                }
                writer.commit();
            }
            printProgress("WritesFinished", writesStart, VALUES, VALUES);
        }

        readIterator(engine);

        try (SqlCompiler compiler = new SqlCompiler(engine)) {
            compiler.compile("dtop table 'keyValue';", ctx);
        }

        engine.close();
    }

    private void readIterator(final CairoEngine engine) throws InterruptedException, SqlException {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final Instant readsStart = new Instant();
        final SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1);
        try (SqlCompiler compiler = new SqlCompiler(engine)) {
            for (int reads = 1; reads <= READS; reads++) {
                FDate prevValue = null;
                int count = 0;
                try (RecordCursorFactory factory = compiler.compile("keyValue", ctx).getRecordCursorFactory()) {
                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                        final io.questdb.cairo.sql.Record record = cursor.getRecord();
                        while (cursor.hasNext()) {
                            final FDate value = new FDate(cursor.getRecord().getLong(0));
                            if (prevValue != null) {
                                Assertions.checkTrue(prevValue.isBefore(value));
                            }
                            prevValue = value;
                            count++;
                        }
                    }
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
