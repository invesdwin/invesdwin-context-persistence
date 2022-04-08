package de.invesdwin.context.persistence.timeseriesdb.updater;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.concurrent.pool.timeout.ATimeoutObjectPool;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
final class TimeseriesUpdaterBatchArrayPool extends ATimeoutObjectPool<Object[]> {

    public static final TimeseriesUpdaterBatchArrayPool INSTANCE = new TimeseriesUpdaterBatchArrayPool();

    private TimeseriesUpdaterBatchArrayPool() {
        super(Duration.ONE_MINUTE, new Duration(10, FTimeUnit.SECONDS));
    }

    @Override
    public void invalidateObject(final Object[] element) {
    }

    @Override
    protected Object[] newObject() {
        return new Object[ATimeSeriesUpdater.BATCH_FLUSH_INTERVAL];
    }

    @Override
    protected void passivateObject(final Object[] obj) {
    }

}
