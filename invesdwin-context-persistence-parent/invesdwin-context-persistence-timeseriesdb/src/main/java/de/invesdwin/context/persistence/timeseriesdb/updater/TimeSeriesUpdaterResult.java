package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.concurrent.graph.WaitForGraph;
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
    //wait-for graph key of the pinned update (null when this result owns no heartbeat lock). The pin was already registered by the updater at heartbeat-lock acquisition (the chained-wait danger lives in the doUpdate() BEFORE this constructor exists); recording the owner thread here keeps the pin release correct even when the warning-finalizer closes this result on another thread
    private final String graphKey;
    private final Thread pinOwnerThread;

    public TimeSeriesUpdaterResult(final FDate updatedTo, final HeartbeatFileChannelLock updateLock,
            final File updateProgressFile, final File updateFinishedFile) {
        this(updatedTo, updateLock, updateProgressFile, updateFinishedFile, null);
    }

    public TimeSeriesUpdaterResult(final FDate updatedTo, final HeartbeatFileChannelLock updateLock,
            final File updateProgressFile, final File updateFinishedFile, final String graphKey) {
        this.updatedTo = updatedTo;
        this.finalizer = new TimeSeriesUpdaterResultFinalizer(updateLock);
        this.updateProgressFile = updateProgressFile;
        this.updateFinishedFile = updateFinishedFile;
        this.graphKey = updateLock != null ? graphKey : null;
        this.pinOwnerThread = this.graphKey != null ? Thread.currentThread() : null;
        finalizer.register(this);
    }

    private void releasePinCount() {
        if (graphKey != null) {
            WaitForGraph.pinReleased(graphKey, pinOwnerThread);
        }
    }

    public FDate getUpdatedTo() {
        return updatedTo;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        try {
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
        } finally {
            // Always release the heartbeat FileChannel and its registry entry, even if the progress
            // file move above threw. Otherwise the leaked FileChannel keeps holding the OS file lock
            // while HeartbeatFileChannelLockRegistry keeps beating its heartbeat time-based and
            // independent of the owner thread's progress, which livelocks every later updater of the
            // same key (trackRemoteUpdate can never steal a lock held by this very JVM).
            releasePinCount();
            finalizer.close();
            closed = true;
        }
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
