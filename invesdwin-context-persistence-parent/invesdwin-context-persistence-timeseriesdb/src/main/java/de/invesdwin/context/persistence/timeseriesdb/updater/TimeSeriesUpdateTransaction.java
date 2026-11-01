package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannel;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.directory.hashkey.version.ITimeSeriesDirectoryHashKeyVersion;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummarySerde;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFiles;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.AMemoryFileSummarySerializingCollection;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.ITimeSeriesMemoryFileLookupTable;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.list.Lists;
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
     * Non-numeric sub directory of a version directory holding the staged index snapshots of live update attempts
     * (epochs). It must stay outside the version's data directory ('memory') so that neither the generation dir-max
     * scanner nor the version dir-max scanner can mistake staged content for published content.
     */
    public static final String EPOCH_STAGING_DIRECTORY_NAME = "epoch-staging";
    public static final String EPOCH_INDEX_SNAPSHOT_FILE_NAME = "index.snapshot";

    private final TimeSeriesLookupStorageCache<?, V> parent;
    private final FDate updateFrom;
    private final List<V> lastValues;
    private final long precedingMemoryOffset;
    private final long memoryOffset;
    private final long precedingValueCount;
    private final String epochLocalDataId;
    private final List<MemoryFileSummary> stagedSummaries;
    private AMemoryFileSummarySerializingCollection summaries;
    private MemoryFileSummary prevSummary;
    //publish-on-commit: close() without an explicit commit() DISCARDS this attempt's staged index (partial results
    //of a failed/aborted update must never become visible; the previously published state simply remains)
    private boolean committed;

    public TimeSeriesUpdateTransaction(final TimeSeriesLookupStorageCache<?, V> parent, final FDate updateFrom,
            final List<V> lastValues, final long precedingMemoryOffset, final long memoryOffset,
            final long precedingValueCount) {
        this(parent, updateFrom, lastValues, precedingMemoryOffset, memoryOffset, precedingValueCount, null);
    }

    public TimeSeriesUpdateTransaction(final TimeSeriesLookupStorageCache<?, V> parent, final FDate updateFrom,
            final List<V> lastValues, final long precedingMemoryOffset, final long memoryOffset,
            final long precedingValueCount, final String epochLocalDataId) {
        this.parent = parent;
        this.updateFrom = updateFrom;
        Assertions.checkNotNull(lastValues);
        this.lastValues = lastValues;
        this.precedingMemoryOffset = precedingMemoryOffset;
        this.memoryOffset = memoryOffset;
        this.precedingValueCount = precedingValueCount;
        this.epochLocalDataId = epochLocalDataId;
        this.stagedSummaries = new ArrayList<>();
        MemoryFiles.assertMaybeFirstSummary(precedingMemoryOffset, memoryOffset, precedingValueCount);
    }

    /**
     * Identifies the data files this attempt writes, or null when it shares the version directory's
     * <code>memory.data</code> file (the legacy layout).
     */
    public String getEpochLocalDataId() {
        return epochLocalDataId;
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
        stagedSummaries.add(summary);
        prevSummary = summary;
    }

    /**
     * Atomically writes the index snapshot of everything this epoch has staged so far into
     * '&lt;versionDir&gt;/epoch-staging/&lt;epochId&gt;/index.snapshot' (temp+rename via AtomicNioFileChannel, the same
     * proven publish primitive as the index generations). Readers that are entitled to see an in-progress epoch (the
     * adoption path of their own demand tree) open this file; it is always a complete, closed collection, never a
     * partially-appended one. Inert when this attempt has no epoch id (legacy layout or the flag off).
     */
    public void writeEpochIndexSnapshot() {
        if (epochLocalDataId == null || stagedSummaries.isEmpty()) {
            return;
        }
        final File stagingDirectory = getEpochStagingDirectory(parent, epochLocalDataId);
        try {
            Files.forceMkdir(stagingDirectory);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        final File snapshotFile = new File(stagingDirectory, EPOCH_INDEX_SNAPSHOT_FILE_NAME);
        try (AMemoryFileSummarySerializingCollection snapshot = newSnapshotCollection(
                new TextDescription("%s.writeEpochIndexSnapshot",
                        TimeSeriesUpdateTransaction.class.getSimpleName()),
                parent, snapshotFile, false)) {
            for (final MemoryFileSummary summary : stagedSummaries) {
                snapshot.add(summary);
            }
            //close() of the AtomicNioFileChannel upload stream does the atomic temp+rename publish
            snapshot.closeWithEmptyWrite();
        }
    }

    /**
     * Reads all MemoryFileSummary records an epoch has staged so far, or null when the epoch has no snapshot yet (no
     * flush landed). Tail-safe: the snapshot file only ever appears or is replaced via an atomic rename of a fully
     * written collection.
     */
    public static List<MemoryFileSummary> readEpochIndexSnapshot(final TimeSeriesLookupStorageCache<?, ?> cache,
            final String epochId) {
        final File snapshotFile = new File(getEpochStagingDirectory(cache, epochId), EPOCH_INDEX_SNAPSHOT_FILE_NAME);
        if (!snapshotFile.exists()) {
            return null;
        }
        final List<MemoryFileSummary> staged = new ArrayList<>();
        try (AMemoryFileSummarySerializingCollection snapshot = newSnapshotCollection(
                new TextDescription("%s.readEpochIndexSnapshot", TimeSeriesUpdateTransaction.class.getSimpleName()),
                cache, snapshotFile, true);
                ICloseableIterator<MemoryFileSummary> iterator = snapshot.iterator()) {
            Lists.toListWithoutHasNext(iterator, staged);
        }
        return staged;
    }

    public static File getEpochStagingDirectory(final TimeSeriesLookupStorageCache<?, ?> cache, final String epochId) {
        Assertions.checkNotNull(epochId);
        final ITimeSeriesDirectoryHashKeyVersion directoryHashKeyVersion = cache.getDirectoryHashKey()
                .getDirectoryHashKeyVersion();
        return new File(
                new File(directoryHashKeyVersion.getDirectoryHashKeyVersionShared(), EPOCH_STAGING_DIRECTORY_NAME),
                epochId);
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

    public synchronized void commit() {
        this.committed = true;
    }

    @Override
    public synchronized void close() {
        if (prevSummary == null) {
            stagedSummaries.clear();
            return;
        }
        if (!committed) {
            //abort path: release the staged collection WITHOUT publishing it into the lookup table and without
            //touching the update marker - a failed attempt changes nothing observable (epoch design I2)
            final AMemoryFileSummarySerializingCollection summariesCopy = summaries;
            if (summariesCopy != null) {
                summariesCopy.closeWithEmptyWrite();
                summariesCopy.clear();
                summaries = null;
            }
            stagedSummaries.clear();
            prevSummary = null;
            return;
        }
        final AMemoryFileSummarySerializingCollection summariesCopy = summaries;
        if (summariesCopy != null) {
            summariesCopy.closeWithEmptyWrite();
            final ITimeSeriesMemoryFileLookupTable memoryFileLookupTable = parent.getMemoryFileLookupTable();
            final ICloseableIterator<MemoryFileSummary> iterator = summariesCopy.iterator();
            memoryFileLookupTable.put(iterator);
            summariesCopy.clear();
            summaries = null;
        }
        stagedSummaries.clear();
        parent.clearCaches();
        touchUpdateMarker(parent.getDirectoryHashKey().getDirectoryHashKeyVersion());
        prevSummary = null;
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

    /**
     * The snapshot collection over the existing MemoryFileSummarySerde path. The serde parameters must be captured as
     * locals before the constructor runs: SerializingCollection calls newSerde() virtually from its own constructor,
     * so a subclass field would still be null there (the anonymous-class pattern mirrors finishFile()).
     */
    private static AMemoryFileSummarySerializingCollection newSnapshotCollection(final TextDescription name,
            final TimeSeriesLookupStorageCache<?, ?> cache, final File snapshotFile, final boolean readOnly) {
        final Integer valueFixedLength = cache.getValueFixedLength();
        final ICompressionFactory compressionFactory = cache.getCompressionFactory();
        return new AMemoryFileSummarySerializingCollection(name,
                AtomicNioFileChannel.newFile(snapshotFile.toURI()), readOnly) {

            @Override
            protected MemoryFileSummarySerde newSerde() {
                return new MemoryFileSummarySerde(valueFixedLength);
            }

            @Override
            protected ICompressionFactory getCompressionFactory() {
                return compressionFactory;
            }
        };
    }

}
