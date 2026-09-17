package de.invesdwin.context.persistence.timeseriesdb.updater;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLock;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.streams.closeable.ISafeCloseable;
import de.invesdwin.util.time.date.FDate;

@Immutable
public class TimeSeriesUpdaterResult implements ISafeCloseable {

    private final FDate updatedTo;
    private final TimeSeriesUpdaterResultFinalizer finalizer;

    public TimeSeriesUpdaterResult(final FDate updatedTo, final HeartbeatFileChannelLock updateLock) {
        this.updatedTo = updatedTo;
        this.finalizer = new TimeSeriesUpdaterResultFinalizer(updateLock);
        finalizer.register(this);
    }

    public FDate getUpdatedTo() {
        return updatedTo;
    }

    @Override
    public void close() {
        finalizer.close();
    }

    private static final class TimeSeriesUpdaterResultFinalizer extends AFinalizer {

        private HeartbeatFileChannelLock updateLock;

        private TimeSeriesUpdaterResultFinalizer(final HeartbeatFileChannelLock updateLock) {
            this.updateLock = updateLock;
        }

        @Override
        protected void clean() {
            final HeartbeatFileChannelLock updateLockCopy = updateLock;
            if (updateLockCopy != null) {
                updateLockCopy.close();
                updateLock = null;
            }
        }

        @Override
        protected boolean isCleaned() {
            return updateLock == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }

    }

}
