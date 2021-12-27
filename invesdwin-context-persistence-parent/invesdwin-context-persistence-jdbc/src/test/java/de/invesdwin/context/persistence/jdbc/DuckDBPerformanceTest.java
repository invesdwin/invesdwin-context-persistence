package de.invesdwin.context.persistence.jdbc;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

/**
 * https://duckdb.org/docs/api/java
 *
 * @author subes
 *
 */
@NotThreadSafe
//@Disabled("manual test")
public class DuckDBPerformanceTest extends ADatabasePerformanceTest {

    @Test
    public void testDuckDbPerformance() throws Exception {
        final File directory = new File(ContextProperties.getCacheDirectory(),
                DuckDBPerformanceTest.class.getSimpleName());
        Files.deleteNative(directory);
        Files.forceMkdirParent(directory);
        final Instant writesStart = new Instant();
        int i = 0;

        Class.forName("org.duckdb.DuckDBDriver");
        final Connection conn = DriverManager.getConnection("jdbc:duckdb:" + directory.getAbsolutePath());
        final Statement create = conn.createStatement();
        create.execute("CREATE TABLE abc (key LONG, value LONG, PRIMARY KEY(key))");
        create.execute("CREATE UNIQUE INDEX idx_abc on abc (key)");
        create.close();
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final Statement insert = conn.createStatement();
        insert.execute("BEGIN TRANSACTION");
        final StringBuilder insertValues = new StringBuilder();
        for (final FDate date : newValues()) {
            final long value = date.longValue(FTimeUnit.MILLISECONDS);
            if (insertValues.length() > 0) {
                insertValues.append(",");
            }
            insertValues.append("(" + value + "," + value + ")");
            i++;
            if (i % FLUSH_INTERVAL == 0) {
                insert.execute("INSERT INTO abc VALUES " + insertValues);
                insertValues.setLength(0);
                if (loopCheck.check()) {
                    printProgress("Writes", writesStart, i, VALUES);
                }
            }
        }
        if (insertValues.length() > 0) {
            insert.execute("INSERT INTO abc VALUES " + insertValues);
            insertValues.setLength(0);
        }
        insert.execute("COMMIT");
        printProgress("WritesFinished", writesStart, VALUES, VALUES);
        insert.close();

        readIterator(conn);
//        readGet(conn);
        readGetLatest(conn);

        final Statement drop = conn.createStatement();
        drop.execute("drop table abc");
        drop.close();

        Files.deleteNative(directory);
    }

    private void readIterator(final Connection conn) throws Exception {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final Instant readsStart = new Instant();
        final Statement select = conn.createStatement();
        for (int reads = 1; reads <= READS; reads++) {
            FDate prevValue = null;
            int count = 0;
            try (ResultSet results = select.executeQuery("SELECT value FROM abc ORDER BY KEY ASC")) {
                while (results.next()) {
                    final FDate value = new FDate(results.getLong(1));
                    if (prevValue != null) {
                        Assertions.checkTrue(prevValue.isBefore(value));
                    }
                    prevValue = value;
                    count++;
                }
            }
            Assertions.checkEquals(count, VALUES);
            if (loopCheck.check()) {
                printProgress("Reads", readsStart, VALUES * reads, VALUES * READS);
            }
        }

        printProgress("ReadsFinished", readsStart, VALUES * READS, VALUES * READS);
    }

    private void readGet(final Connection conn) throws Exception {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final List<FDate> values = Lists.toList(newValues());
        final Instant readsStart = new Instant();
        for (int reads = 0; reads < READS; reads++) {
            FDate prevValue = null;
            int count = 0;
            try (Statement select = conn.createStatement()) {
                for (int i = 0; i < values.size(); i++) {
                    try (ResultSet results = select.executeQuery(
                            "SELECT value FROM abc where key = " + values.get(i).millisValue() + " LIMIT 1")) {
                        Assertions.checkTrue(results.next());
                        final FDate value = new FDate(results.getLong(1));
                        if (prevValue != null) {
                            Assertions.checkTrue(prevValue.isBefore(value));
                        }
                        prevValue = value;
                        count++;
                        if (loopCheck.check()) {
                            printProgress("Reads", readsStart, VALUES * reads + count, VALUES * READS);
                        }
                    }
                }
            }
            Assertions.checkEquals(count, VALUES);
        }

        printProgress("GetsFinished", readsStart, VALUES * READS, VALUES * READS);
    }

    private void readGetLatest(final Connection conn) throws Exception {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final List<FDate> values = Lists.toList(newValues());
        final Instant readsStart = new Instant();
        for (int reads = 0; reads < READS; reads++) {
            FDate prevValue = null;
            int count = 0;
            try (Statement select = conn.createStatement()) {
                for (int i = 0; i < values.size(); i++) {
                    try (ResultSet results = select.executeQuery(
                            "SELECT max(value) FROM abc WHERE key <= " + values.get(i).millisValue() + " LIMIT 1")) {
                        Assertions.checkTrue(results.next());
                        final FDate value = new FDate(results.getLong(1));
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
        printProgress("GetLatestsFinished", readsStart, VALUES * READS, VALUES * READS);
    }
}
