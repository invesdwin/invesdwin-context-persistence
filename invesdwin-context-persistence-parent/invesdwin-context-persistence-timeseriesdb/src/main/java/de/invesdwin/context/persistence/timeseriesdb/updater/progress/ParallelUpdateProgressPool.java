package de.invesdwin.context.persistence.timeseriesdb.updater.progress;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.concurrent.pool.timeout.ATimeoutObjectPool;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@SuppressWarnings("rawtypes")
@ThreadSafe
final class ParallelUpdateProgressPool extends ATimeoutObjectPool<ParallelUpdateProgress> {

    public static final ParallelUpdateProgressPool INSTANCE = new ParallelUpdateProgressPool();

    private ParallelUpdateProgressPool() {
        super(Duration.ONE_MINUTE, new Duration(10, FTimeUnit.SECONDS));
    }

    @Override
    public void invalidateObject(final ParallelUpdateProgress element) {
        element.reset();
    }

    @Override
    protected ParallelUpdateProgress newObject() {
        return new ParallelUpdateProgress();
    }

    @Override
    protected boolean passivateObject(final ParallelUpdateProgress element) {
        element.reset();
        return true;
    }

}
