package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannel;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.ITimeSeriesDirectoryHashKeyVersion;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummarySerde;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFiles;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.AMemoryFileSummarySerializingCollection;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.ITimeSeriesMemoryFileLookupTable;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.concurrent.lock.file.AtomicNioFileChannelContext;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.string.Charsets;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.streams.closeable.ISafeCloseable;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.millis.FDateMillis;

@NotThreadSafe
public class TimeSeriesUpdateTransaction<V> implements ISafeCloseable {

    private final TimeSeriesLookupStorageCache<?, V> parent;
    private final FDate updateFrom;
    private final List<V> lastValues;
    private final long precedingMemoryOffset;
    private final long memoryOffset;
    private final long precedingValueCount;
    private final String epochLocalDataId;
    private final FDate resumeAfterEndTime;
    private AMemoryFileSummarySerializingCollection summaries;
    private MemoryFileSummary prevSummary;
    //publish-on-commit: close() without an explicit commit() DISCARDS only what this attempt staged after its last
    //published boundary; a final commit() publishes the rest
    private boolean committed;
    //segments staged since the last publish, the unit a boundary commit publishes
    private int stagedSegmentCount;

    public TimeSeriesUpdateTransaction(final TimeSeriesLookupStorageCache<?, V> parent, final FDate updateFrom,
            final List<V> lastValues, final long precedingMemoryOffset, final long memoryOffset,
            final long precedingValueCount, final String epochLocalDataId, final FDate resumeAfterEndTime) {
        this.parent = parent;
        this.updateFrom = updateFrom;
        Assertions.checkNotNull(lastValues);
        this.lastValues = lastValues;
        this.precedingMemoryOffset = precedingMemoryOffset;
        this.memoryOffset = memoryOffset;
        this.precedingValueCount = precedingValueCount;
        this.epochLocalDataId = epochLocalDataId;
        this.resumeAfterEndTime = resumeAfterEndTime;
        MemoryFiles.assertMaybeFirstSummary(precedingMemoryOffset, memoryOffset, precedingValueCount);
    }

    /**
     * Identifies the data files this attempt writes; never null, the epoch local layout is the only supported layout.
     */
    public String getEpochLocalDataId() {
        return epochLocalDataId;
    }

    /**
     * The end time of the last value the published generation already holds when this attempt continues it (a resumed
     * attempt - the previous attempt died after a boundary commit), null when the attempt starts from an empty store or
     * redoes the incomplete last file instead. A source that hands that value out again must not have it written
     * twice, so the attempt's writer skips everything up to and including it.
     */
    public FDate getResumeAfterEndTime() {
        return resumeAfterEndTime;
    }

    public TimeSeriesLookupStorageCache<?, V> getParent() {
        return parent;
    }

    public FDate getUpdateFrom() {
        return updateFrom;
    }

    public List<V> getLastValues() {
        return lastValues;
    }

    public long getPrecedingMemoryOffset() {
        return precedingMemoryOffset;
    }

    public long getMemoryOffset() {
        return memoryOffset;
    }

    public long getPrecedingValueCount() {
        return precedingValueCount;
    }

    public void finishFile(final V firstValue, final V lastValue, final long precedingValueCount, final int valueCount,
            final File memoryFile, final long precedingMemoryOffset, final long memoryOffset, final long memoryLength) {
        final FDate firstValueEndTime = parent.extractEndTime(firstValue);
        final MemoryFileSummary summary = new MemoryFileSummary(firstValueEndTime, parent.getValueSerde(), firstValue,
                lastValue, precedingValueCount, valueCount, memoryFile.getAbsolutePath(), precedingMemoryOffset,
                memoryOffset, memoryLength);
        finishFile(summary);
    }

    public void finishFile(final MemoryFileSummary summary) {
        assertSummaryBeforeCommit(summary);
        if (summaries == null) {
            final File tempSummariesFile = new File(
                    parent.getDirectoryHashKeyVersionMemory().getDirectoryHashKeyVersionDataShared(),
                    AMemoryFileSummarySerializingCollection.MEMORY_INDEX_FILE_NAME + ".update"
                            + AtomicNioFileChannelContext.TMP_EXTENSION);
            summaries = new AMemoryFileSummarySerializingCollection(
                    new TextDescription("%s.finishFile", TimeSeriesUpdateTransaction.class.getSimpleName()),
                    AtomicNioFileChannel.newFile(tempSummariesFile.toURI()), false) {

                @Override
                protected MemoryFileSummarySerde newSerde() {
                    return new MemoryFileSummarySerde(parent.getValueFixedLength());
                }

                @Override
                protected ICompressionFactory getCompressionFactory() {
                    return parent.getCompressionFactory();
                }
            };
        }
        summaries.add(summary);
        prevSummary = summary;
        stagedSegmentCount++;
    }

    private void assertSummaryBeforeCommit(final MemoryFileSummary summary) {
        final MemoryFileSummary lastSummary;
        if (prevSummary == null) {
            lastSummary = parent.getLatestRangeKeyCompleteOnly();
        } else {
            lastSummary = prevSummary;
        }
        parent.assertSummary(lastSummary, summary);
    }

    /**
     * Boundary commit of a completed segment: publishes everything this attempt has staged so far as a new index
     * generation and lets the attempt continue staging into it, so a long backfill publishes incrementally and an
     * attempt that dies later leaves a readable high-water mark behind instead of nothing. The progress writers call
     * this after every completed segment; a publish only actually happens once
     * {@link TimeSeriesProperties#UPDATE_COMMIT_SEGMENT_INTERVAL} segments have piled up, because every publish writes
     * a new index generation.
     *
     * @return true when this call published - the caller must then stop appending to the data file it has been writing
     *         and continue in a new one, so that a published file never carries bytes of a segment that was not
     *         published (a file with unpublished trailing bytes would make the store fail its own length check in
     *         {@link TimeSeriesLookupStorageCache#isEmptyOrInconsistent()} and get reset)
     */
    public synchronized boolean commitSegmentBoundary() {
        if (committed || stagedSegmentCount < TimeSeriesProperties.UPDATE_COMMIT_SEGMENT_INTERVAL) {
            return false;
        }
        publishStagedSummaries();
        return true;
    }

    /**
     * Marks the attempt as finished: its remaining staged segments (the incomplete last file) are published by
     * {@link #close()}.
     */
    public synchronized void commit() {
        this.committed = true;
    }

    @Override
    public synchronized void close() {
        if (prevSummary == null) {
            return;
        }
        if (committed) {
            publishStagedSummaries();
        } else {
            //abort path: only what this attempt staged after its last published boundary is released WITHOUT
            //publishing it; the boundaries it did publish stay, and the next attempt of this key continues from them
            final AMemoryFileSummarySerializingCollection summariesCopy = summaries;
            if (summariesCopy != null) {
                summariesCopy.closeWithEmptyWrite();
                summariesCopy.clear();
                summaries = null;
                stagedSegmentCount = 0;
            }
        }
        prevSummary = null;
    }

    private void publishStagedSummaries() {
        final AMemoryFileSummarySerializingCollection summariesCopy = summaries;
        if (summariesCopy != null) {
            summariesCopy.closeWithEmptyWrite();
            final ITimeSeriesMemoryFileLookupTable memoryFileLookupTable = parent.getMemoryFileLookupTable();
            final ICloseableIterator<MemoryFileSummary> iterator = summariesCopy.iterator();
            memoryFileLookupTable.put(iterator);
            summariesCopy.clear();
            summaries = null;
            stagedSegmentCount = 0;
        }
        parent.clearCaches();
        touchUpdateMarker(parent.getDirectoryHashKey().getDirectoryHashKeyVersion());
    }

    public static void touchUpdateMarker(final ITimeSeriesDirectoryHashKeyVersion directoryHashKeyVersion) {
        // mark the directory as populated (since we already own the file lock the populate it)
        final File updatedMarkerFile = directoryHashKeyVersion.getUpdatedMarkerFile();
        touchUpdateMarker(updatedMarkerFile);
    }

    public static void touchUpdateMarker(final File updatedMarkerFile) {
        if (!updatedMarkerFile.exists()) {
            try {
                Files.writeStringToFile(updatedMarkerFile, "Created: " + FDate.now(), Charsets.defaultCharset());
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            updatedMarkerFile.setLastModified(FDateMillis.nowMillis());
        }
    }

}
