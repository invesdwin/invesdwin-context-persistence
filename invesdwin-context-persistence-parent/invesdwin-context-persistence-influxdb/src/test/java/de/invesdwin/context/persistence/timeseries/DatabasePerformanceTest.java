package de.invesdwin.context.persistence.timeseries;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import javax.annotation.concurrent.NotThreadSafe;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;
import org.junit.Ignore;
import org.junit.Test;

import de.flapdoodle.embed.process.runtime.Network;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.concurrent.reference.IMutableReference;
import de.invesdwin.util.concurrent.reference.VolatileReference;
import de.invesdwin.util.lang.ProcessedEventsRateString;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.math.decimal.scaled.PercentScale;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;
import io.apisense.embed.influx.InfluxServer;
import io.apisense.embed.influx.configuration.InfluxConfigurationWriter;

@NotThreadSafe
@Ignore("manual test")
public class DatabasePerformanceTest extends ATest {

    private static final int READS = 10;
    private static final int VALUES = 1_000_000;
    private static final String HASH_KEY = "HASH_KEY";
    private static final int FLUSH_INTERVAL = 10_000;

    @Test
    public void testInfluxDbPerformanceAsync() throws Exception {
        final int freeHttpPort = Network.getFreeServerPort();
        final int freeUdpPort = Network.getFreeServerPort();

        final InfluxServer server = startInfluxDB(freeHttpPort, freeUdpPort);
        server.start();
        try {
            Thread.sleep(10 * 1000);

            final String dbname = "influxDbPerformance";
            final String policyname = "defaultPolicy";
            final String measurementName = "measurementsPerformance";

            final InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:" + freeHttpPort);
            influxDB.createDatabase(dbname);
            influxDB.createRetentionPolicy(policyname, dbname, "9999d", 1, true);

            BatchPoints batch = BatchPoints.database(dbname).retentionPolicy(policyname).build();
            final Instant writesStart = new Instant();
            int i = 0;
            for (final FDate date : newValues()) {
                final Point point = Point.measurement(measurementName)
                        .time(date.millisValue(), TimeUnit.MILLISECONDS)
                        .tag("hashKey", HASH_KEY)
                        .addField("value", date.millisValue())
                        .build();

                batch.point(point);
                i++;
                if (i % FLUSH_INTERVAL == 0) {
                    printProgress("Writes", writesStart, i, VALUES);
                    influxDB.write(batch);
                    batch = BatchPoints.database(dbname).retentionPolicy(policyname).build();
                }
            }
            influxDB.write(batch);
            batch = null;
            printProgress("WritesFinished", writesStart, VALUES, VALUES);

            TimeUnit.SECONDS.sleep(1);

            final Instant readsStart = new Instant();
            for (int reads = 1; reads <= READS; reads++) {
                final IMutableReference<FDate> prevValueRef = new VolatileReference<>();
                final AtomicBoolean finished = new AtomicBoolean();
                final int readsFinal = reads;
                final AtomicInteger count = new AtomicInteger();
                influxDB.query(new Query("Select value from " + measurementName + " where hashKey = '" + HASH_KEY + "'",
                        dbname), 10_000, new Consumer<QueryResult>() {

                            @Override
                            public void accept(final QueryResult queryResult) {
                                try {
                                    final List<Result> range = queryResult.getResults();
                                    if (range == null || range.isEmpty()) {
                                        return;
                                    }
                                    final Result firstResult = range.get(0);
                                    if (firstResult == null) {
                                        return;
                                    }
                                    final List<Series> series = firstResult.getSeries();
                                    if (series == null || series.isEmpty()) {
                                        return;
                                    }
                                    final List<List<Object>> firstSeries = series.get(0).getValues();
                                    if (firstSeries == null) {
                                        return;
                                    }
                                    for (int result = 0; result < firstSeries.size(); result++) {
                                        final Double valueDouble = (Double) firstSeries.get(result).get(1);
                                        final FDate value = new FDate(valueDouble.longValue());
                                        final FDate prevValue = prevValueRef.get();
                                        if (prevValue != null) {
                                            Assertions.checkTrue(prevValue.isBefore(value));
                                        }
                                        prevValueRef.set(value);
                                        count.incrementAndGet();
                                    }
                                } catch (final Throwable t) {
                                    Err.process(t);
                                }
                            }
                        }, () -> finished.set(true));
                while (!finished.get() && count.get() != VALUES) {
                    TimeUnit.NANOSECONDS.sleep(1);
                }
                Assertions.checkEquals(count.get(), VALUES);
                printProgress("Reads", readsStart, VALUES * readsFinal, VALUES * READS);
            }
            printProgress("ReadsFinished", readsStart, VALUES * READS, VALUES * READS);
        } finally {
            server.stop();
        }
    }

    @Test
    public void testInfluxDbPerformanceSync() throws Exception {
        final int freeHttpPort = Network.getFreeServerPort();
        final int freeUdpPort = Network.getFreeServerPort();

        final InfluxServer server = startInfluxDB(freeHttpPort, freeUdpPort);
        server.start();
        try {
            Thread.sleep(10 * 1000);

            final String dbname = "influxDbPerformance";
            final String policyname = "defaultPolicy";
            final String measurementName = "measurementsPerformance";

            final InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:" + freeHttpPort);
            influxDB.createDatabase(dbname);
            influxDB.createRetentionPolicy(policyname, dbname, "9999d", 1, true);

            BatchPoints batch = BatchPoints.database(dbname).retentionPolicy(policyname).build();
            final Instant writesStart = new Instant();
            int i = 0;
            for (final FDate date : newValues()) {
                final Point point = Point.measurement(measurementName)
                        .time(date.millisValue(), TimeUnit.MILLISECONDS)
                        .tag("hashKey", HASH_KEY)
                        .addField("value", date.millisValue())
                        .build();

                batch.point(point);
                i++;
                if (i % FLUSH_INTERVAL == 0) {
                    printProgress("Writes", writesStart, i, VALUES);
                    influxDB.write(batch);
                    batch = BatchPoints.database(dbname).retentionPolicy(policyname).build();
                }
            }
            influxDB.write(batch);
            batch = null;
            printProgress("WritesFinished", writesStart, VALUES, VALUES);

            final Instant readsStart = new Instant();
            for (int reads = 1; reads <= READS; reads++) {
                FDate prevValue = null;
                final QueryResult queryResult = influxDB
                        .query(new Query("Select value from " + measurementName + " where hashKey = '" + HASH_KEY + "'",
                                dbname), TimeUnit.MILLISECONDS);

                final List<Result> range = queryResult.getResults();
                int count = 0;
                final List<List<Object>> series = range.get(0).getSeries().get(0).getValues();
                for (int result = 0; result < series.size(); result++) {
                    final Double valueDouble = (Double) series.get(result).get(0);
                    final FDate value = new FDate(valueDouble.longValue());
                    if (prevValue != null) {
                        Assertions.checkTrue(prevValue.isBefore(value));
                    }
                    prevValue = value;
                    count++;
                }
                Assertions.checkEquals(count, VALUES);
                printProgress("Reads", readsStart, VALUES * reads, VALUES * READS);
            }
            printProgress("ReadsFinished", readsStart, VALUES * READS, VALUES * READS);
        } finally {
            server.stop();
        }
    }

    private InfluxServer startInfluxDB(final int freeHttpPort, final int freeUdpPort) throws IOException {
        final InfluxServer.Builder builder = new InfluxServer.Builder();
        // configuration to start InfluxDB server with HTTP on port `freeHttpPort`
        // and default backup restore port
        final InfluxConfigurationWriter influxConfig = new InfluxConfigurationWriter.Builder().setHttp(freeHttpPort) // by default auth is disabled
                .setUdp(freeUdpPort) // If you happen to need udp enabled, by default to 'udp' database
                .build();

        builder.setInfluxConfiguration(influxConfig); // let's start both of protocols, HTTP and UDP
        final InfluxServer server = builder.build();
        return server;
    }

    private void printProgress(final String action, final Instant start, final long count, final int maxCount) {
        final Duration duration = start.toDuration();
        log.info("%s: %s/%s (%s) %s during %s", action, count, maxCount,
                new Percent(count, maxCount).toString(PercentScale.PERCENT),
                new ProcessedEventsRateString(count, duration), duration);
    }

    private ICloseableIterable<FDate> newValues() {
        final FDate reference = new FDate();
        return FDates.iterable(reference, reference.addMilliseconds(VALUES - 1), FTimeUnit.MILLISECONDS, 1);
    }

}
