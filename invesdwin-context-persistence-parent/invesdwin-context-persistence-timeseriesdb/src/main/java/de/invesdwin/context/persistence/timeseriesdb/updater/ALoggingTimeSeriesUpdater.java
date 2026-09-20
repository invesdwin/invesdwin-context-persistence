package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdateProgress;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLockRegistry;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.string.ProcessedEventsRateString;
import de.invesdwin.util.math.Longs;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.math.decimal.scaled.PercentScale;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.date.millis.FDateNanos;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public abstract class ALoggingTimeSeriesUpdater<K, V> extends ATimeSeriesUpdater<K, V> {

    public static final int BATCH_LOG_INTERVAL = 100_000 / ATimeSeriesUpdater.DEFAULT_BATCH_FLUSH_INTERVAL;
    public static final Duration ELEMENT_LOG_INTERVAL = Duration.FIVE_SECONDS;
    public static final Duration FLUSH_LOG_INTERVAL = Duration.ONE_SECOND;

    private final Log log;
    private final AtomicLong lastFlushIndex = new AtomicLong();
    @GuardedBy("this")
    private FDate updateStart;
    @GuardedBy("this")
    private FDate lastFlushMaxTime;
    @GuardedBy("this")
    private long lastLogFlushTimeNanos = Instant.DUMMY_NANOS;

    private final ILock elementLock = ILockCollectionFactory.getInstance(true)
            .newLock(ALoggingTimeSeriesUpdater.class.getSimpleName() + "_elementLock");
    private final AtomicLong elementCount = new AtomicLong();
    private volatile String owner = HeartbeatFileChannelLockRegistry.HEARTBEAT_OWNER;
    @GuardedBy("elementLock")
    private long lastLogElementTimeNanos = Instant.DUMMY_NANOS;
    @GuardedBy("elementLock")
    private FDate elementMinTime;
    @GuardedBy("elementLock")
    private FDate elementMaxTime;

    public ALoggingTimeSeriesUpdater(final K key, final ATimeSeriesDB<K, V> table, final Log log) {
        super(key, table);
        this.log = log;
    }

    @Override
    protected void onUpdateStarted(final FDate updateStart) {
        log.info("Updating %s for [%s]", getElementsName(), keyToString(getKey()));
        this.updateStart = updateStart;
    }

    @Override
    protected void onElement(final ITimeSeriesUpdateProgress relativeProgress, final long relativeCount) {
        final FDate minTime = relativeProgress.getMinTime();
        if (minTime == null) {
            return;
        }
        final long elements = elementCount.addAndGet(relativeCount);
        if (elementMinTime == null) {
            elementMinTime = minTime;
            if (lastLogElementTimeNanos == Instant.DUMMY_NANOS) {
                lastLogElementTimeNanos = FDateNanos.elapsedNanos();
            }
        }
        if (elementLock.tryLock()) {
            try {
                final long nowNanos = FDateNanos.elapsedNanos();
                if (shouldLogElements(nowNanos)) {
                    elementMinTime = FDates.min(elementMinTime, minTime);
                    elementMaxTime = FDates.max(elementMaxTime, relativeProgress.getMaxTime());
                    logElements(nowNanos, elements);
                }
            } finally {
                elementLock.unlock();
            }
        }
    }

    private boolean shouldLogElements(final long nowNanos) {
        //if we are too fast, only print status once a second
        return ELEMENT_LOG_INTERVAL.isLessThanNanos(nowNanos - lastLogFlushTimeNanos)
                && ELEMENT_LOG_INTERVAL.isLessThanNanos(nowNanos - lastLogElementTimeNanos);
    }

    private void logElements(final long nowNanos, final long elements) {
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
        lastLogElementTimeNanos = nowNanos;
    }

    @Override
    protected synchronized void onFlush(final ITimeSeriesUpdateProgress relativeProgress, final long flushIndex) {
        owner = relativeProgress.getOwner();
        final long prevFlushIndex = lastFlushIndex.get();
        lastFlushIndex.set(Longs.max(prevFlushIndex, flushIndex));
        lastFlushMaxTime = FDates.max(lastFlushMaxTime, relativeProgress.getMaxTime());
        final long flushIncrement = flushIndex - prevFlushIndex;
        if (flushIncrement > BATCH_LOG_INTERVAL) {
            logFlush(FLUSH_LOG_INTERVAL);
        } else if (flushIndex != prevFlushIndex) {
            logFlush(ELEMENT_LOG_INTERVAL);
        }
    }

    private void logFlush(final Duration logInterval) {
        final long nowNanos = FDateNanos.elapsedNanos();
        //if we are too fast, only print status once a second
        if (logInterval.isLessThanNanos(nowNanos - lastLogFlushTimeNanos)
                && logInterval.isLessThanNanos(nowNanos - lastLogElementTimeNanos)) {
            final Duration flushDuration = updateStart.toDuration();
            final Percent progress = getProgress();
            if (progress != null) {
                log.info("%sPersisted %s. %s batch for [%s]. Reached [%s] at time [%s]. Processed [%s] during %s",
                        newOwnerPrefix(owner), lastFlushIndex, getElementsName(), keyToString(getKey()),
                        progress.asScale(PercentScale.PERCENT), lastFlushMaxTime,
                        new ProcessedEventsRateString(getValueCount(), flushDuration), flushDuration);
            } else {
                log.info("%sPersisted %s. %s batch for [%s]. Reached time [%s]. Processed [%s] during %s",
                        newOwnerPrefix(owner), lastFlushIndex, getElementsName(), keyToString(getKey()),
                        lastFlushMaxTime, new ProcessedEventsRateString(getValueCount(), flushDuration), flushDuration);
            }
            lastLogFlushTimeNanos = nowNanos;
        }
    }

    public static String newOwnerPrefix(final String owner) {
        final String ownerPrefix;
        if (Objects.equals(owner, HeartbeatFileChannelLockRegistry.HEARTBEAT_OWNER)) {
            ownerPrefix = "";
        } else {
            ownerPrefix = "[" + owner + "] ";
        }
        return ownerPrefix;
    }

    @Override
    protected synchronized void onUpdateFinished() {
        if (lastFlushIndex != null) {
            logFlush(FLUSH_LOG_INTERVAL);
        }
        log.info("%sFinished updating %s %s for [%s] from [%s] to [%s] after %s", newOwnerPrefix(owner),
                getValueCount(), getElementsName(), keyToString(getKey()), getMinTime(), getMaxTime(),
                updateStart.toDuration());
    }

    protected abstract String keyToString(K key);

    protected abstract String getElementsName();

}