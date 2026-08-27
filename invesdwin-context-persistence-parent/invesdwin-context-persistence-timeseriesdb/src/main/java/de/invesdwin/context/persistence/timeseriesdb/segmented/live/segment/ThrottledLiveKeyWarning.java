package de.invesdwin.context.persistence.timeseriesdb.segmented.live.segment;

import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.collections.eviction.EvictionMode;
import de.invesdwin.util.concurrent.loop.DisabledLoopInterruptedCheck;
import de.invesdwin.util.concurrent.loop.ILoopInterruptedCheck;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.log.ILog;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class ThrottledLiveKeyWarning {

    private final ILog log;
    private final Set<FDate> nextLiveStartTime_lastWarnings = Collections
            .newSetFromMap(EvictionMode.LeastRecentlyAdded.newMap(10));
    private final ILoopInterruptedCheck loopInterruptedCheck;
    private long skippedWarnings = 0;

    public ThrottledLiveKeyWarning(final ILog log) {
        this.log = log;
        if (Throwables.isDebugStackTraceEnabled()) {
            this.loopInterruptedCheck = DisabledLoopInterruptedCheck.INSTANCE;
        } else {
            this.loopInterruptedCheck = new LoopInterruptedCheck();
        }
    }

    public void maybeWarn(final FDate nextLiveStartTime, final FDate lastLiveKey, final Object name) {
        if (!log.isWarnEnabled()) {
            return;
        }
        if (!loopInterruptedCheck.checkClockNoInterrupt()) {
            skippedWarnings++;
        }
        if (nextLiveStartTime_lastWarnings.add(nextLiveStartTime)) {
            final StringBuilder sb = new StringBuilder();
            sb.append("nextLiveStartTime [");
            sb.append(nextLiveStartTime);
            sb.append("] should be after or equal to lastLiveKey [");
            sb.append(lastLiveKey);
            sb.append("]");
            if (skippedWarnings > 0) {
                sb.append(" (");
                sb.append(skippedWarnings);
                sb.append(" more warnings skipped)");
            }
            sb.append(": ");
            sb.append(name);
            log.warn(sb.toString());
            skippedWarnings = 0;
        }
    }

}
