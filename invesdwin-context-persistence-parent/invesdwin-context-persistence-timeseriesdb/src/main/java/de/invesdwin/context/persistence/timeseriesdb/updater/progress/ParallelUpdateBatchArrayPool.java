package de.invesdwin.context.persistence.timeseriesdb.updater.progress;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.util.concurrent.pool.timeout.ATimeoutObjectPool;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
final class ParallelUpdateBatchArrayPool extends ATimeoutObjectPool<Object[]> {

    public static final ParallelUpdateBatchArrayPool INSTANCE = new ParallelUpdateBatchArrayPool();

    private ParallelUpdateBatchArrayPool() {
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
