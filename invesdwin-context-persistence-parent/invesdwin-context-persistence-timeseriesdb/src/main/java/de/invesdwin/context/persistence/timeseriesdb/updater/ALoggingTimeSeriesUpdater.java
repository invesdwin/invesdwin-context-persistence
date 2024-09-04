package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.IUpdateProgress;
import de.invesdwin.util.lang.string.ProcessedEventsRateString;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.math.decimal.scaled.PercentScale;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public abstract class ALoggingTimeSeriesUpdater<K, V> extends ATimeSeriesUpdater<K, V> {

    public static final int BATCH_LOG_INTERVAL = 100_000 / ATimeSeriesUpdater.DEFAULT_BATCH_FLUSH_INTERVAL;
    public static final Duration ELEMENT_LOG_INTERVAL = Duration.FIVE_SECONDS;

    private final Log log;
    private final AtomicInteger lastFlushIndex = new AtomicInteger();
    @GuardedBy("this")
    private Instant updateStart;
    @GuardedBy("this")
    private long flushElementCount;
    @GuardedBy("this")
    private FDate lastFlushMaxTime;
    @GuardedBy("this")
    private Instant lastLogFlushTime;

    private final Object elementLock = new Object();
    private final AtomicLong elementCount = new AtomicLong();
    private Instant lastLogElementTime = new Instant();
    @GuardedBy("elementLock")
    private FDate elementMinTime;
    @GuardedBy("elementLock")
    private FDate elementMaxTime;

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
    protected void onElement(final IUpdateProgress<K, V> progress) {
        final long elements = elementCount.incrementAndGet();
        if (elementMinTime == null) {
            elementMinTime = progress.getMinTime();
        }
        if (shouldLogElements()) {
            synchronized (elementLock) {
                if (shouldLogElements()) {
                    elementMinTime = FDates.min(elementMinTime, progress.getMinTime());
                    elementMaxTime = FDates.max(elementMaxTime, progress.getMaxTime());
                    logElements(elements);
                }
            }
        }
    }

    private boolean shouldLogElements() {
        return (lastLogFlushTime == null || lastLogFlushTime.isGreaterThan(ELEMENT_LOG_INTERVAL))
                //if we are too fast, only print status once a second
                && (lastLogElementTime == null || lastLogElementTime.isGreaterThan(ELEMENT_LOG_INTERVAL));
    }

    private void logElements(final long elements) {
        final Duration flushDuration = updateStart.toDuration();
        final Percent progress = getProgress(elementMinTime, elementMaxTime);
        if (progress != null) {
            log.info("Persisting %s. %s batch for [%s]. Reached [%s] at time [%s]. Processed [%s] during %s",
                    lastFlushIndex.intValue() + 1, getElementsName(), keyToString(getKey()),
                    progress.asScale(PercentScale.PERCENT), elementMaxTime,
                    new ProcessedEventsRateString(elements, flushDuration), flushDuration);
        } else {
            log.info("Persisting %s. %s batch for [%s]. Reached time [%s]. Processed [%s] during %s",
                    lastFlushIndex.intValue() + 1, getElementsName(), keyToString(getKey()), elementMaxTime,
                    new ProcessedEventsRateString(elements, flushDuration), flushDuration);
        }
        lastLogElementTime = new Instant();
    }

    @Override
    protected synchronized void onFlush(final int flushIndex, final IUpdateProgress<K, V> progress) {
        lastFlushIndex.set(Integers.max(lastFlushIndex.get(), flushIndex));
        flushElementCount += progress.getValueCount();
        lastFlushMaxTime = FDates.max(lastFlushMaxTime, progress.getMaxTime());
        if (flushIndex % BATCH_LOG_INTERVAL == 0) {
            logFlush();
        }
    }

    private void logFlush() {
        //if we are too fast, only print status once a second
        if (lastLogFlushTime == null || lastLogFlushTime.isGreaterThan(Duration.ONE_SECOND)) {
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
            lastLogFlushTime = new Instant();
        }
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