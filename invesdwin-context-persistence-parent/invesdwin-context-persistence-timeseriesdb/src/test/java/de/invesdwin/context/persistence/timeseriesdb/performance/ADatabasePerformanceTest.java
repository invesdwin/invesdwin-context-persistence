package de.invesdwin.context.persistence.timeseriesdb.performance;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.lang.string.ProcessedEventsRateString;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.math.decimal.scaled.PercentScale;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public abstract class ADatabasePerformanceTest extends ATest {

    //    static {
    //makes it sloooooow
    //        LZ4Streams.setAllowJniCompressor(true);
    //    }

    protected static final long READS = 100;
    protected static final long VALUES = 1_000_000;
    protected static final String HASH_KEY = "HASH_KEY";
    protected static final int FLUSH_INTERVAL = ATimeSeriesUpdater.DEFAULT_BATCH_FLUSH_INTERVAL;

    protected void printProgress(final String action, final Instant start, final long count, final long maxCount) {
        final Duration duration = start.toDuration();
        log.info("%s: %s/%s (%s) %s during %s", action, count, maxCount,
                new Percent(count, maxCount).toString(PercentScale.PERCENT),
                new ProcessedEventsRateString(count, duration), duration);
    }

    protected ICloseableIterable<FDate> newValues() {
        return FDates.iterable(FDates.MIN_DATE, FDates.MIN_DATE.addMilliseconds(VALUES - 1), FTimeUnit.MILLISECONDS, 1);
    }

}
