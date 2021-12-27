package de.invesdwin.context.persistence.jdbc;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
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
public class HsqldbPerformanceTest extends ADatabasePerformanceTest {

    @Test
    public void testDuckDbPerformance() throws Exception {
        final File directory = new File(ContextProperties.getCacheDirectory(),
                HsqldbPerformanceTest.class.getSimpleName());
        Files.deleteNative(directory);
        Files.forceMkdirParent(directory);
        final Instant writesStart = new Instant();
        int i = 0;

        Class.forName("org.duckdb.DuckDBDriver");
        final Connection conn = DriverManager.getConnection("jdbc:duckdb:" + directory.getAbsolutePath());
        final Statement create = conn.createStatement();
        create.execute("CREATE TABLE abc (key TIMESTAMP, value LONG, PRIMARY KEY(key))");
        create.close();
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final PreparedStatement insert = conn.prepareStatement("INSERT INTO abc VALUES (?,?)");
        for (final FDate date : newValues()) {
            final long value = date.longValue(FTimeUnit.MILLISECONDS);
            insert.setDate(1, date.sqlDateValue());
            insert.setLong(2, value);
            insert.addBatch();
            i++;
            if (i % FLUSH_INTERVAL == 0) {
                insert.executeBatch();
                if (loopCheck.check()) {
                    printProgress("Writes", writesStart, i, VALUES);
                }
            }
        }
        insert.executeBatch();
        printProgress("WritesFinished", writesStart, VALUES, VALUES);
        insert.close();

        readIterator(conn);
        readGet(conn);
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
            try (ResultSet results = select.executeQuery("SELECT value FROM abc")) {
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
            try (PreparedStatement select = conn
                    .prepareStatement("SELECT value FROM abc where key = cast(? AS TIMESTAMP) LIMIT 1")) {
                for (int i = 0; i < values.size(); i++) {
                    select.setLong(1, i);
                    try (ResultSet results = select.executeQuery()) {
                        Assertions.checkTrue(results.next());
                        final FDate value = new FDate(results.getLong(1));
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

        printProgress("GetsFinished", readsStart, VALUES * READS, VALUES * READS);
    }

    private void readGetLatest(final Connection conn) throws Exception {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final List<FDate> values = Lists.toList(newValues());
        final Instant readsStart = new Instant();
        for (int reads = 0; reads < READS; reads++) {
            FDate prevValue = null;
            int count = 0;
            try (PreparedStatement select = conn
                    .prepareStatement("SELECT max(value) FROM abc WHERE key <= cast(? AS TIMESTAMP) LIMIT 1")) {
                for (int i = 0; i < values.size(); i++) {
                    select.setLong(1, values.get(i).millisValue());
                    try (ResultSet results = select.executeQuery()) {
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
