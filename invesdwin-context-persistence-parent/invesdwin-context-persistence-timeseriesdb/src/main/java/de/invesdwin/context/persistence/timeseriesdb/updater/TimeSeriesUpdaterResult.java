package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLock;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.finalizer.AWarningFinalizer;
import de.invesdwin.util.streams.closeable.ISafeCloseable;
import de.invesdwin.util.time.date.FDate;

@Immutable
public class TimeSeriesUpdaterResult implements ISafeCloseable {

    private final FDate updatedTo;
    private final TimeSeriesUpdaterResultFinalizer finalizer;
    private boolean closed;
    private final File updateProgressFile;
    private final File updateFinishedFile;

    public TimeSeriesUpdaterResult(final FDate updatedTo, final HeartbeatFileChannelLock updateLock,
            final File updateProgressFile, final File updateFinishedFile) {
        this.updatedTo = updatedTo;
        this.finalizer = new TimeSeriesUpdaterResultFinalizer(updateLock);
        this.updateProgressFile = updateProgressFile;
        this.updateFinishedFile = updateFinishedFile;
        finalizer.register(this);
    }

    public FDate getUpdatedTo() {
        return updatedTo;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        if (updateProgressFile != null && updateFinishedFile != null) {
            try {
                Files.moveFile(updateProgressFile, updateFinishedFile);
            } catch (final FileNotFoundException e) {
                // ignore, file was already moved or deleted
                Files.touchQuietly(updateFinishedFile);
            } catch (final IOException e) {
                throw new RuntimeException("Failed to move update progress file to update finished file", e);
            }
        }
        finalizer.close();
        closed = true;
    }

    private static final class TimeSeriesUpdaterResultFinalizer extends AWarningFinalizer {

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
