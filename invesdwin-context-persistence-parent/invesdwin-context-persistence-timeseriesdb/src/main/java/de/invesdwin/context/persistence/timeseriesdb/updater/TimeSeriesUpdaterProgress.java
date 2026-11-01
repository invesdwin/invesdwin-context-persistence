package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
    /**
     * The epoch (update attempt) this marker was written by, or null for a marker written by an attempt that does not
     * know about epochs (the legacy 6 field format). The serialized form carries it as an optional 7th field; the same
     * field may additionally hold the epochs this attempt consumed as further comma-separated entries
     * ('&lt;key&gt;#&lt;epochId&gt;'), so old readers simply reject the extra field (they require exactly 6) while new
     * readers tolerate 6 or 7 fields.
     */
    private final String epochId;
    private final List<String> consumedEpochEntries;

    public TimeSeriesUpdaterProgress(final String owner, final FDate updateStart, final long flushIndex,
            final long valueCount, final FDate minTime, final FDate maxTime) {
        this(owner, updateStart, flushIndex, valueCount, minTime, maxTime, null, null);
    }

    public TimeSeriesUpdaterProgress(final String owner, final FDate updateStart, final long flushIndex,
            final long valueCount, final FDate minTime, final FDate maxTime, final String epochId,
            final List<String> consumedEpochEntries) {
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
        this.epochId = Strings.isBlank(epochId) ? null : epochId;
        this.consumedEpochEntries = consumedEpochEntries != null ? new ArrayList<>(consumedEpochEntries)
                : new ArrayList<>();
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

    /**
     * The id of the update attempt (epoch) that wrote this marker, or null when the marker carries no epoch
     * information (legacy format).
     */
    public String getEpochId() {
        return epochId;
    }

    /**
     * The epochs consumed by this attempt as '&lt;key&gt;#&lt;epochId&gt;' entries; empty when nothing was consumed.
     * A demanding pass records an entry here whenever it adopts a sibling's staged epoch snapshot, so that a later
     * commit-time re-check can validate that the consumed epochs are still current (the re-check itself is a
     * follow-up milestone).
     */
    public List<String> getConsumedEpochEntries() {
        return consumedEpochEntries;
    }

    public static void writeUpdateProgress(final File updateProgressFile, final TimeSeriesUpdaterProgress progress) {
        writeUpdateProgress(updateProgressFile, progress.getUpdateStart(), progress.getFlushIndex(),
                progress.getValueCount(), progress.getMinTime(), progress.getMaxTime(), progress.getEpochId(),
                progress.getConsumedEpochEntries());
    }

    public static void writeUpdateProgress(final File updateProgressFile, final FDate updateStart,
            final long flushIndex, final long valueCount, final FDate minTime, final FDate maxTime) {
        writeUpdateProgress(updateProgressFile, updateStart, flushIndex, valueCount, minTime, maxTime, null, null);
    }

    public static void writeUpdateProgress(final File updateProgressFile, final FDate updateStart,
            final long flushIndex, final long valueCount, final FDate minTime, final FDate maxTime,
            final String epochId, final List<String> consumedEpochEntries) {
        try {
            final StringBuilder content = new StringBuilder(
                    HeartbeatFileChannelLockRegistry.HEARTBEAT_OWNER + ";"
                            + Strings.nullToEmpty(FDates.toString(updateStart, TIME_FORMAT)) + ";" + flushIndex + ";"
                            + valueCount + ";" + Strings.nullToEmpty(FDates.toString(minTime, TIME_FORMAT)) + ";"
                            + Strings.nullToEmpty(FDates.toString(maxTime, TIME_FORMAT)));
            final boolean hasConsumed = consumedEpochEntries != null && !consumedEpochEntries.isEmpty();
            if (epochId != null || hasConsumed) {
                //optional 7th field: the epoch id, optionally followed by comma-joined '<key>#<epochId>' entries
                content.append(";").append(Strings.nullToEmpty(epochId));
                if (hasConsumed) {
                    content.append(",").append(Strings.join(consumedEpochEntries, ','));
                }
            }
            Files.writeStringToFile(updateProgressFile, content.toString(), Charsets.defaultCharset());
        } catch (final IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public static TimeSeriesUpdaterProgress readUpdateProgress(final File updateProgressFile) {
        try {
            final String progress = Files.readFileToStringNoThrow(updateProgressFile, Charsets.defaultCharset());
            if (Strings.isBlank(progress)) {
                return null;
            }
            final String[] split = Strings.splitPreserveAllTokens(progress, ";");
            //tolerant: 6 fields are the legacy format, 7 fields carry the epoch (and possibly consumed epochs)
            if (split.length != 6 && split.length != 7) {
                return null;
            }
            final String owner = split[0];
            final FDate updateStart = FDate.valueOf(split[1], TIME_FORMAT);
            final long flushIndex = Long.parseLong(split[2]);
            final long valueCount = Long.parseLong(split[3]);
            final FDate minTime = FDate.valueOf(split[4], TIME_FORMAT);
            final FDate maxTime = FDate.valueOf(split[5], TIME_FORMAT);
            String epochId = null;
            List<String> consumedEpochEntries = null;
            if (split.length == 7) {
                final String[] epochSplit = Strings.splitPreserveAllTokens(split[6], ",");
                epochId = epochSplit.length > 0 && Strings.isNotBlank(epochSplit[0]) ? epochSplit[0] : null;
                if (epochSplit.length > 1) {
                    consumedEpochEntries = new ArrayList<>(epochSplit.length - 1);
                    for (int i = 1; i < epochSplit.length; i++) {
                        if (Strings.isNotBlank(epochSplit[i])) {
                            consumedEpochEntries.add(epochSplit[i]);
                        }
                    }
                }
            }
            return new TimeSeriesUpdaterProgress(owner, updateStart, flushIndex, valueCount, minTime, maxTime, epochId,
                    consumedEpochEntries);
        } catch (final Throwable e) {
            return null;
        }
    }

    public TimeSeriesUpdaterProgress asRelativeProgress(final TimeSeriesUpdaterProgress prevProgress) {
        final long relativeValueCount = valueCount - prevProgress.getValueCount();
        return new TimeSeriesUpdaterProgress(owner, updateStart, flushIndex, relativeValueCount, minTime, maxTime,
                epochId, consumedEpochEntries);
    }

}
