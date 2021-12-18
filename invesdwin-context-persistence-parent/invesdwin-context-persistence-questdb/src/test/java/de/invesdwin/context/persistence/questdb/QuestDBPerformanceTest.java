package de.invesdwin.context.persistence.questdb;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.lang.Files;
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
@Disabled("manual test")
public class QuestDBPerformanceTest extends ADatabasePerformanceTest {

    @Test
    public void testQuestDbPerformance() throws InterruptedException, SqlException, IOException {
        final File directory = new File(ContextProperties.getCacheDirectory(),
                QuestDBPerformanceTest.class.getSimpleName());
        Files.deleteNative(directory);
        Files.forceMkdir(directory);
        final CairoConfiguration configuration = new DefaultCairoConfiguration(directory.getAbsolutePath());
        final Instant writesStart = new Instant();
        int i = 0;
        final CairoEngine engine = new CairoEngine(configuration);
        final SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1);
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        try (SqlCompiler compiler = new SqlCompiler(engine)) {

            compiler.compile("create table abc (value long, key timestamp) timestamp(key)", ctx);

            try (TableWriter writer = engine.getWriter(ctx.getCairoSecurityContext(), "abc", "insert")) {
                for (final FDate date : newValues()) {
                    final TableWriter.Row row = writer.newRow(date.millisValue());
                    row.putLong(0, date.millisValue());
                    row.append();
                    i++;
                    if (i % FLUSH_INTERVAL == 0) {
                        if (loopCheck.check()) {
                            printProgress("Writes", writesStart, i, VALUES);
                        }
                        writer.commit();
                    }
                }
                writer.commit();
            }
            printProgress("WritesFinished", writesStart, VALUES, VALUES);
        }

        readIterator(engine);
        readGet(engine);
        readGetLatest(engine);

        try (SqlCompiler compiler = new SqlCompiler(engine)) {
            compiler.compile("dtop table 'abc';", ctx);
        }

        engine.close();

        Files.deleteNative(directory);
    }

    private void readIterator(final CairoEngine engine) throws InterruptedException, SqlException {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final Instant readsStart = new Instant();
        final SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1);
        try (SqlCompiler compiler = new SqlCompiler(engine)) {
            for (int reads = 1; reads <= READS; reads++) {
                FDate prevValue = null;
                int count = 0;
                try (RecordCursorFactory factory = compiler.compile("abc", ctx).getRecordCursorFactory()) {
                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                        final io.questdb.cairo.sql.Record record = cursor.getRecord();
                        while (cursor.hasNext()) {
                            final FDate value = new FDate(record.getLong(0));
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

    private void readGet(final CairoEngine engine) throws InterruptedException, SqlException {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final List<FDate> values = Lists.toList(newValues());
        final Instant readsStart = new Instant();
        final SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1);
        try (SqlCompiler compiler = new SqlCompiler(engine)) {
            for (int reads = 0; reads < READS; reads++) {
                FDate prevValue = null;
                int count = 0;
                for (int i = 0; i < values.size(); i++) {
                    try (RecordCursorFactory factory = compiler
                            .compile("abc WHERE key = cast(" + values.get(i).millisValue() + " AS TIMESTAMP) LIMIT 1",
                                    ctx)
                            .getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(ctx)) {
                            final io.questdb.cairo.sql.Record record = cursor.getRecord();
                            Assertions.checkTrue(cursor.hasNext());
                            final FDate value = new FDate(record.getLong(0));
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
                    printProgress("Gets", readsStart, VALUES * reads, VALUES * READS);
                }
            }
        }
        printProgress("GetsFinished", readsStart, VALUES * READS, VALUES * READS);
    }

    private void readGetLatest(final CairoEngine engine) throws InterruptedException, SqlException {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final List<FDate> values = Lists.toList(newValues());
        final Instant readsStart = new Instant();
        final SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1);
        try (SqlCompiler compiler = new SqlCompiler(engine)) {
            for (int reads = 0; reads < READS; reads++) {
                FDate prevValue = null;
                int count = 0;
                for (int i = 0; i < values.size(); i++) {
                    //ORDER BY key DESC is horribly slow (90/s), SELECT max works better
                    try (RecordCursorFactory factory = compiler.compile("SELECT max(value) FROM abc WHERE key <= cast("
                            + (values.get(i).millisValue()) + " AS TIMESTAMP) LIMIT 1", ctx).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(ctx)) {
                            final io.questdb.cairo.sql.Record record = cursor.getRecord();
                            Assertions.checkTrue(cursor.hasNext());
                            final FDate value = new FDate(record.getLong(0));
                            if (prevValue != null) {
                                Assertions.checkTrue(prevValue.isBefore(value));
                            }
                            prevValue = value;
                            count++;
                            if (loopCheck.check()) {
                                printProgress("GetLatests", readsStart, VALUES * reads + count, VALUES * READS);
                            }
                        }
                    }
                }
                Assertions.checkEquals(count, VALUES);
            }
        }
        printProgress("GetLatestsFinished", readsStart, VALUES * READS, VALUES * READS);
    }
}
