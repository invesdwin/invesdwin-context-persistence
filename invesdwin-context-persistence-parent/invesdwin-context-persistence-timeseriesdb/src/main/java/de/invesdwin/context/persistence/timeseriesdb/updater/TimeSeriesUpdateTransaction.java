package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannel;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateYieldException;
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

    /**
     * Completed segments one index generation amortises over. This is a constant of the layout, not a deployment
     * setting: as a system property it let the boxes in this fleet drift apart, and what it trades is a property of the
     * format. Measured on lap-mid 2026-11-14 (one committed pass of 4,000 segments, 400,000 values of a 5-byte FDate,
     * batch flush 100, parallel writer, ext4, JDK 17.0.20.1), sweeping the cadence through the then-live property:
     * <pre>
     *   segments per publish        1        10        25        50
     *   generations              4,000       400       160        80
     *   index bytes            1,977,295   276,783   148,007   128,277
     *   index share of bytes        48 %      11 %       6 %       5 %
     *   payload files              4,000       401       161        81
     *   pass wall time        41,102 ms   4,705 ms   2,655 ms   2,014 ms
     * </pre>
     * Coarser is cheaper for the writer, and the read side does not suffer either way (the live chain range is bounded
     * by {@code CHAIN_MAX_DELTA_GENERATIONS}, measured at 144-160 generations across this sweep). What a publish is
     * still needed for, and what keeps this from going coarser: it is the CRASH-loss granularity of a pass (an attempt
     * that ends by exception lands its finished batches at {@link #close()}, a killed JVM keeps only what was already
     * published), and it is how far the frontier consumers attached through {@link #setBoundaryCommitListener} can see
     * while a pass runs. 10 puts a live derived-bars boundary at 10 x 10,000 values, which is the granularity the
     * demand side is paced against.
     */
    static final int PUBLISH_SEGMENTS_PER_GENERATION = 10;

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
    //the outcome of the pass: true when the source was consumed to the end without an exception. It does NOT decide
    //whether the staged work lands - close() publishes what this attempt FINISHED on either path - it only decides
    //what the attempt's one memory.log record states
    private boolean committed;
    //segments staged since the last publish, the unit a boundary commit publishes
    private int stagedSegmentCount;
    //what this attempt landed and published, reported ONCE when its transaction closes - whatever the outcome
    private int dataFilesStaged;
    private int publishes;
    private MemoryFileSummary lastPublishedSummary;
    private long publishedChainHighWater;
    //the attempt's table write lock lease, attached only by an attempt that holds the key's write lock
    //(ATimeSeriesUpdater.update); the boundary commit yields it after publishing, see commitSegmentBoundary
    private TimeSeriesWriteLockYield writeLockYield;
    //the boundary-commit notification of consumers that mirror this store's frontier (null = none): fired after
    //every publish, before the publish-then-yield, so frontier keys advance with published segments
    private ITimeSeriesBoundaryCommitListener boundaryCommitListener;

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
        dataFilesStaged++;
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
     * generation and lets the attempt continue staging into it. What a boundary commit buys over the publish
     * {@link #close()} does anyway is a readable high-water mark DURING a pass that may run for hours, so the frontier
     * consumers attached through {@link #setBoundaryCommitListener} advance while the attempt is still running. The
     * progress writers call this after every completed segment; a publish (and with it the write-lock yield) only
     * actually happens once {@link #PUBLISH_SEGMENTS_PER_GENERATION} segments have piled up.
     *
     * The yield rides the publish cadence on purpose. Releasing the write lock costs a grace sleep - see the WINDOW_GRACE_MILLIS
     * grace sleep in {@link TimeSeriesWriteLockYield} - because on this non-fair read-write lock a writer that
     * releases and immediately re-acquires wins its own race and the "window" is no window. Yielding at every completed
     * segment was measured at 23,358 ms for a pass that costs 4,705 ms when the yield rides the publish (lap-mid
     * 2026-11-14, one 4,000 segment pass, identical index bytes and identical payload file counts - the whole delta is
     * grace sleep). Since the pass holds the key's update lock throughout, making a pass 5x longer makes every reader
     * of that key wait longer in aggregate, not less: finer windows are not a fairer store.
     *
     * @return true when this call published - the caller must then stop appending to the data file it has been writing
     *         and continue in a new one, so that a published file never carries bytes of a segment that was not
     *         published (a file with unpublished trailing bytes would make the store fail its own length check in
     *         {@link TimeSeriesLookupStorageCache#isEmptyOrInconsistent()} and get reset)
     * @throws IncompleteUpdateYieldException when this attempt holds a table write lock lease
     *         ({@link #setWriteLockYield}) and could not re-acquire the lock after publishing: the publish-then-yield
     *         of this boundary ends the attempt CLEANLY, and close() publishes whatever this pass finished, so nothing
     *         it landed is lost
     */
    public synchronized boolean commitSegmentBoundary() {
        if (committed || stagedSegmentCount < PUBLISH_SEGMENTS_PER_GENERATION) {
            return false;
        }
        publishStagedSummaries();
        //the publish just landed, so the store's readable frontier is now the last published value: consumers
        //mirroring it (downloaded frontier keys of the caching services above) get told before this attempt does
        //anything else, including yielding the write lock below
        if (boundaryCommitListener != null) {
            boundaryCommitListener.onBoundaryCommitPublished(parent.getLastValueEndTime());
        }
        //publish-then-yield: the store's visible state right now is exactly the published generations and this
        //attempt holds no data file open mid-segment (the caller rotates onto a fresh file for the next segment), so
        //releasing the table write lock here gives the readers parked on the key's read side their window while the
        //single-writer invariant keeps holding for everything unpublished
        if (writeLockYield != null && !writeLockYield.yieldAndTryReacquire()) {
            throw new IncompleteUpdateYieldException(
                    "Update attempt of hash key [" + parent.getHashKey() + "] published a segment boundary and yielded"
                            + " the table write lock, but readers of the key kept it past the re-acquisition window of"
                            + " [" + TimeSeriesProperties.UPDATE_YIELD_ACQUIRE_WRITE_LOCK_TIMEOUT + "] - ending the"
                            + " attempt cleanly at this boundary; close() publishes what this pass finished, so the"
                            + " next attempt resumes above it");
        }
        return true;
    }

    /**
     * Attaches the table write lock lease of an attempt that holds the key's table write lock for its pass
     * ({@link ATimeSeriesUpdater#update()}); every boundary commit of the attempt then yields that lock after
     * publishing (publish-then-yield). Consumers that drive a transaction without holding a table write lock (the
     * segmented updater path, index-only writers) leave it unset and their boundary commits keep behaving exactly as
     * before. Must be attached before the attempt stages its first segment.
     */
    void setWriteLockYield(final TimeSeriesWriteLockYield writeLockYield) {
        Assertions.checkTrue(prevSummary == null && !committed,
                "the write lock lease must be attached before the attempt stages a segment");
        this.writeLockYield = writeLockYield;
    }

    /**
     * Attaches the boundary-commit notification of a consumer that mirrors this store's published frontier
     * ({@link ITimeSeriesBoundaryCommitListener}); every publish reports its new frontier to the listener, including
     * the one {@link #close()} does with whatever the attempt staged after its last boundary - committed or aborted,
     * because the frontier the consumer mirrors is what the store now reads. Must be attached before the attempt's
     * first publish; null leaves the transaction silent (unchanged behaviour for consumers that do not use the hook).
     */
    void setBoundaryCommitListener(final ITimeSeriesBoundaryCommitListener boundaryCommitListener) {
        this.boundaryCommitListener = boundaryCommitListener;
    }

    /**
     * Marks the pass as having consumed its source to the end. What the attempt staged lands either way - see
     * {@link #close()} - so this records the OUTCOME for the attempt's log record and for the callers that report a
     * completed pass; it is not a commit point.
     */
    public synchronized void commit() {
        this.committed = true;
    }

    /**
     * Ends the transaction: whatever this attempt FINISHED lands in the published chain, committed or aborted. Every
     * summary in the staged batch names a data file the writer completed - {@link #finishFile} runs only once that file
     * is flushed and closed - so the file still being written is never in the batch and a published file still carries
     * no unpublished trailing bytes. An attempt that dies mid-pass therefore keeps the segments it landed: the next
     * attempt of the key resumes from them instead of re-fetching the same range, which is what the shape this replaces
     * cost on the live box (595 replays of one 2006 to 2021 pass, and the 34,541 '_incomplete_' files the census still
     * counted). The epoch reclaim of an aborted attempt now deletes only files that no published generation names.
     */
    @Override
    public synchronized void close() {
        if (prevSummary == null) {
            return;
        }
        if (publishStagedSummaries() && boundaryCommitListener != null) {
            //the last publish of the attempt moves the store's readable frontier as much as a boundary commit does,
            //so the consumers mirroring it hear about it here too
            boundaryCommitListener.onBoundaryCommitPublished(parent.getLastValueEndTime());
        }
        //ONE record for the attempt, written after the outcome is settled so it states what the store now holds. The
        //early return above - an attempt that staged nothing never gets here - is what implements "nothing added,
        //nothing written", and testEachAttemptLeavesOneLogRecordStatingWhatTheStoreHolds pins it. See
        //MemoryFileMetadata.logTransaction for why this is one record per attempt, not per publish or per summary.
        final FDate lastPublishedValueEndTime = lastPublishedSummary != null
                ? parent.extractEndTime(parent.getValueSerde().fromBytes(lastPublishedSummary.getLastValue()))
                : null;
        parent.getMemoryFileLookupTable().getMetadata().logTransaction(getEpochLocalDataId(), committed,
                dataFilesStaged, publishes, lastPublishedSummary, lastPublishedValueEndTime,
                publishedChainHighWater);
        prevSummary = null;
    }

    /**
     * Writes the staged batch as a new index generation.
     *
     * @return true when a generation was written - false when nothing was staged since the last publish, which is what
     *         a close right after a boundary commit sees; no generation and no cache invalidation then, since the
     *         store's visible state did not change
     */
    private boolean publishStagedSummaries() {
        final AMemoryFileSummarySerializingCollection summariesCopy = summaries;
        if (summariesCopy == null) {
            return false;
        }
        summariesCopy.closeWithEmptyWrite();
        final ITimeSeriesMemoryFileLookupTable memoryFileLookupTable = parent.getMemoryFileLookupTable();
        final ICloseableIterator<MemoryFileSummary> iterator = summariesCopy.iterator();
        memoryFileLookupTable.put(iterator);
        publishes++;
        lastPublishedSummary = prevSummary;
        if (prevSummary != null) {
            publishedChainHighWater = prevSummary.getCombinedMemoryFileSize();
        }
        summariesCopy.clear();
        summaries = null;
        stagedSegmentCount = 0;
        parent.clearCaches();
        touchUpdateMarker(parent.getDirectoryHashKey().getDirectoryHashKeyVersion());
        return true;
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
