package de.invesdwin.context.persistence.timeseriesdb.updater;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.ProcessedEventsRateString;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.math.decimal.scaled.PercentScale;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public abstract class ALoggingTimeSeriesUpdater<K, V> extends ATimeSeriesUpdater<K, V> {

    public static final int BATCH_LOG_INTERVAL = 100_000 / ATimeSeriesUpdater.BATCH_FLUSH_INTERVAL;

    private final Log log;
    @GuardedBy("this")
    private Integer lastFlushIndex;
    @GuardedBy("this")
    private Instant updateStart;
    @GuardedBy("this")
    private long flushElementCount;
    @GuardedBy("this")
    private FDate lastFlushMaxTime;
    @GuardedBy("this")
    private Instant lastFlushTime;

    public ALoggingTimeSeriesUpdater(final K key, final ATimeSeriesDB<K, V> table, final Log log) {
        super(key, table);
        this.log = log;
    }

    @Override
    protected void onUpdateStart() {
        log.info("Updating %s for [%s]", getElementsName(), keyToString(getKey()));
        updateStart = new Instant();
    }

    @Override
    protected synchronized void onFlush(final int flushIndex, final UpdateProgress progress) {
        lastFlushIndex = Integers.max(lastFlushIndex, flushIndex);
        flushElementCount += progress.getValueCount();
        lastFlushMaxTime = FDates.max(lastFlushMaxTime, progress.getMaxTime());
        if (flushIndex % BATCH_LOG_INTERVAL == 0) {
            logFlush();
        }
    }

    private void logFlush() {
        Assertions.assertThat(lastFlushIndex).isNotNull();

        //if we are too fast, only print status once a second
        if (lastFlushTime == null || lastFlushTime.isGreaterThan(Duration.ONE_SECOND)) {
            final Duration flushDuration = updateStart.toDuration();
            final Percent progress = getProgress();
            if (progress != null) {
                log.info("Persisted %s. %s batch for [%s]. Reached [%s] at time [%s]. Processed [%s] during %s",
                        lastFlushIndex, getElementsName(), keyToString(getKey()),
                        progress.asScale(PercentScale.PERCENT), lastFlushMaxTime,
                        new ProcessedEventsRateString(flushElementCount, flushDuration), flushDuration);
            } else {
                log.info("Persisted %s. %s batch for [%s]. Reached time [%s]. Processed [%s] during %s", lastFlushIndex,
                        getElementsName(), keyToString(getKey()), lastFlushMaxTime,
                        new ProcessedEventsRateString(flushElementCount, flushDuration), flushDuration);
            }
            lastFlushTime = new Instant();
        }

        lastFlushIndex = null;
        lastFlushMaxTime = null;
    }

    @Override
    protected synchronized void onUpdateFinished(final Instant updateStart) {
        if (lastFlushIndex != null) {
            logFlush();
        }
        log.info("Finished updating %s %s for [%s] from [%s] to [%s] after %s", getCount(), getElementsName(),
                keyToString(getKey()), getMinTime(), getMaxTime(), updateStart);
    }

    protected abstract String keyToString(K key);

    protected abstract String getElementsName();

}