package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdateProgress;
import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLockRegistry;
import de.invesdwin.util.error.RuntimeIOException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.string.Charsets;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;

@Immutable
public class TimeSeriesUpdaterProgress implements ITimeSeriesUpdateProgress {

    public static final TimeSeriesUpdaterProgress EMPTY = new TimeSeriesUpdaterProgress(
            HeartbeatFileChannelLockRegistry.HEARTBEAT_OWNER, FDates.MIN_DATE, 0, 0, null, null);

    private static final String TIME_FORMAT = FDate.FORMAT_ISO_DATE_TIME_PS;
    private final String owner;
    private final FDate updateStart;
    private final long flushIndex;
    private final long valueCount;
    private final FDate minTime;
    private final FDate maxTime;

    public TimeSeriesUpdaterProgress(final String owner, final FDate updateStart, final long flushIndex,
            final long valueCount, final FDate minTime, final FDate maxTime) {
        this.owner = owner;
        if (owner == null) {
            throw new NullPointerException("owner cannot be null");
        }
        this.updateStart = updateStart;
        if (updateStart == null) {
            throw new NullPointerException("updateStart cannot be null");
        }
        this.flushIndex = flushIndex;
        this.valueCount = valueCount;
        this.minTime = minTime;
        this.maxTime = maxTime;
    }

    @Override
    public String getOwner() {
        return owner;
    }

    public FDate getUpdateStart() {
        return updateStart;
    }

    public long getFlushIndex() {
        return flushIndex;
    }

    @Override
    public long getValueCount() {
        return valueCount;
    }

    @Override
    public FDate getMinTime() {
        return minTime;
    }

    @Override
    public FDate getMaxTime() {
        return maxTime;
    }

    public static void writeUpdateProgress(final File updateProgressFile, final TimeSeriesUpdaterProgress progress) {
        writeUpdateProgress(updateProgressFile, progress.getUpdateStart(), progress.getFlushIndex(),
                progress.getValueCount(), progress.getMinTime(), progress.getMaxTime());
    }

    public static void writeUpdateProgress(final File updateProgressFile, final FDate updateStart,
            final long flushIndex, final long valueCount, final FDate minTime, final FDate maxTime) {
        try {
            Files.writeStringToFile(updateProgressFile,
                    HeartbeatFileChannelLockRegistry.HEARTBEAT_OWNER + ";"
                            + Strings.nullToEmpty(FDates.toString(updateStart, TIME_FORMAT)) + ";" + flushIndex + ";"
                            + valueCount + ";" + Strings.nullToEmpty(FDates.toString(minTime, TIME_FORMAT)) + ";"
                            + Strings.nullToEmpty(FDates.toString(maxTime, TIME_FORMAT)),
                    Charsets.defaultCharset());
        } catch (final IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public static TimeSeriesUpdaterProgress readUpdateProgress(final File updateProgressFile) {
        try {
            final String progress = Files.readFileToStringNoThrow(updateProgressFile, Charsets.defaultCharset());
            final String[] split = Strings.splitPreserveAllTokens(progress, ";");
            if (split.length != 6) {
                return null;
            }
            final String owner = split[0];
            final FDate updateStart = FDate.valueOf(split[1], TIME_FORMAT);
            final long flushIndex = Long.parseLong(split[2]);
            final long valueCount = Long.parseLong(split[3]);
            final FDate minTime = FDate.valueOf(split[4], TIME_FORMAT);
            final FDate maxTime = FDate.valueOf(split[5], TIME_FORMAT);
            return new TimeSeriesUpdaterProgress(owner, updateStart, flushIndex, valueCount, minTime, maxTime);
        } catch (final Throwable e) {
            return null;
        }
    }

    public TimeSeriesUpdaterProgress asRelativeProgress(final TimeSeriesUpdaterProgress prevProgress) {
        final long relativeValueCount = valueCount - prevProgress.getValueCount();
        return new TimeSeriesUpdaterProgress(owner, updateStart, flushIndex, relativeValueCount, minTime, maxTime);
    }

}
