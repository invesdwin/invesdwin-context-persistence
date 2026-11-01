package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.Immutable;
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
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.concurrent.lock.file.AtomicNioFileChannelContext;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.string.Charsets;
import de.invesdwin.util.lang.string.Strings;
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
     * partially-appended one. An epoch that has staged nothing yet still lands an EMPTY snapshot (epoch design
     * M3-cp start-time write): the staging directory and snapshot exist from attempt start on, so the heartbeat and
     * snapshot existence gates of adoption and of consumed re-validation can be satisfied before the first flush.
     * Inert when this attempt has no epoch id (legacy layout or the flag off).
     */
    public void writeEpochIndexSnapshot() {
        if (epochLocalDataId == null) {
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
            //close() of the AtomicNioFileChannel upload stream does the atomic temp+rename publish; closeWithEmptyWrite
            //lands a zero length file when nothing has been staged yet (start-time snapshot)
            snapshot.closeWithEmptyWrite();
        }
    }

    /**
     * Reads all MemoryFileSummary records an epoch has staged so far, or null when the epoch has no snapshot at all
     * (no attempt started for it). Since M3-cp an attempt writes an empty snapshot at start, so a live epoch without a
     * landed flush reads back as an EMPTY list (not null); tail-safe either way: the snapshot file only ever appears or
     * is replaced via an atomic rename of a fully written collection.
     */
    public static List<MemoryFileSummary> readEpochIndexSnapshot(final TimeSeriesLookupStorageCache<?, ?> cache,
            final String epochId) {
        final File snapshotFile = new File(getEpochStagingDirectory(cache, epochId), EPOCH_INDEX_SNAPSHOT_FILE_NAME);
        if (!snapshotFile.exists()) {
            return null;
        }
        return readSummariesOf(cache, snapshotFile);
    }

    /**
     * Opens one summary collection (an epoch index snapshot or a published '&lt;N&gt;_memory.index' generation) of the
     * given store cache over the same serde parameters, read only.
     */
    public static AMemoryFileSummarySerializingCollection readSummaryCollection(
            final TimeSeriesLookupStorageCache<?, ?> cache, final File summariesFile) {
        return newSnapshotCollection(
                new TextDescription("%s.readSummaryCollection", TimeSeriesUpdateTransaction.class.getSimpleName()),
                cache, summariesFile, true);
    }

    private static List<MemoryFileSummary> readSummariesOf(final TimeSeriesLookupStorageCache<?, ?> cache,
            final File summariesFile) {
        final List<MemoryFileSummary> staged = new ArrayList<>();
        try (AMemoryFileSummarySerializingCollection collection = readSummaryCollection(cache, summariesFile);
                ICloseableIterator<MemoryFileSummary> iterator = collection.iterator()) {
            Lists.toListWithoutHasNext(iterator, staged);
        }
        return staged;
    }

    public static File getEpochStagingDirectory(final TimeSeriesLookupStorageCache<?, ?> cache, final String epochId) {
        Assertions.checkNotNull(epochId);
        final ITimeSeriesDirectoryHashKeyVersion directoryHashKeyVersion = cache.getDirectoryHashKey()
                .getDirectoryHashKeyVersion();
        return getEpochStagingDirectory(directoryHashKeyVersion.getDirectoryHashKeyVersionShared(), epochId);
    }

    /**
     * The staging directory of an epoch inside an explicit version directory (file-system level resolution without
     * creating a lease - used by the reclaim sweep and the consumed re-validation of sibling keys).
     */
    public static File getEpochStagingDirectory(final File versionDirectory, final String epochId) {
        Assertions.checkNotNull(epochId);
        return new File(new File(versionDirectory, EPOCH_STAGING_DIRECTORY_NAME), epochId);
    }

    /**
     * State of a consumed epoch ('&lt;key&gt;#&lt;epochId&gt;' ledger entry) at re-validation time (epoch design
     * M3-cp, commit-time consumed re-check for the fixed-point driver). Pure observation, no behavior change:
     * <ul>
     * <li>{@link ConsumedEpochStatus#PUBLISHED}: the epoch's data files are still named by a published index
     * generation of that key - the attempt this demand consumed has landed and nothing has superseded its bytes
     * out of every retained generation;</li>
     * <li>{@link ConsumedEpochStatus#LIVE}: the epoch's attempt is still in flight (its progress marker names it and
     * its heartbeat claim is fresh) and its staged index snapshot still exists;</li>
     * <li>{@link ConsumedEpochStatus#STALE}: neither - the consumed epoch was superseded or aborted, the consuming
     * pass's derived values are no longer fixed-point-consistent and the driver must re-iterate.</li>
     * </ul>
     */
    @Immutable
    public enum ConsumedEpochStatus {
        PUBLISHED,
        LIVE,
        STALE;
    }

    /**
     * Re-validates a consumed-epoch ledger against the current store state (epoch design M3-cp step 6/8: at commit
     * time each member re-checks that the sibling epochs it consumed are still committed-published or
     * live-and-current). The cache may be the consuming key's own store cache - it supplies the table directory and
     * the serde/compression parameters used to read the consumed keys' published generations; the entry key part is
     * resolved to that sibling key's version directory on disk without creating a lease. Returns one status per
     * entry (insertion order), STALE for malformed entries.
     */
    public static Map<String, ConsumedEpochStatus> validateConsumedEpochs(
            final TimeSeriesLookupStorageCache<?, ?> cache, final List<String> entries) {
        final Map<String, ConsumedEpochStatus> statuses = ILockCollectionFactory.getInstance(false).newLinkedMap();
        if (entries == null || entries.isEmpty()) {
            return statuses;
        }
        final File sharedRoot = cache.getDirectoryHashKey().getParent().getDirectoryShared();
        for (final String entry : entries) {
            statuses.put(entry, validateConsumedEpoch(cache, sharedRoot, entry));
        }
        return statuses;
    }

    private static ConsumedEpochStatus validateConsumedEpoch(final TimeSeriesLookupStorageCache<?, ?> cache,
            final File sharedRoot, final String entry) {
        if (Strings.isBlank(entry)) {
            return ConsumedEpochStatus.STALE;
        }
        final String keyStr = EpochScopes.consumedEntryKey(entry);
        final String epochId = EpochScopes.consumedEntryEpochId(entry);
        if (keyStr == null || epochId == null) {
            return ConsumedEpochStatus.STALE;
        }
        //the directory name a key's store lives under is the normalized hash key string (ATimeSeriesDB.hashKeyToString)
        final File keyDirectory = new File(sharedRoot, Files.normalizeFileName(keyStr));
        final File versionDirectory = latestNumericVersionDirectory(keyDirectory);
        if (versionDirectory == null) {
            return ConsumedEpochStatus.STALE;
        }
        final File dataDirectory = new File(versionDirectory, TimeSeriesLookupStorageCache.MEMORY_DATA_ID);
        if (TimeSeriesEpochReclaim.isEpochReferencedByPublishedGenerations(cache, dataDirectory, epochId)) {
            return ConsumedEpochStatus.PUBLISHED;
        }
        final File updateProgressFile = new File(dataDirectory, TimeSeriesLookupStorageCache.UPDATE_PROGRESS_FILE_NAME);
        if (TimeSeriesEpochReclaim.isEpochLive(updateProgressFile, epochId) && new File(
                getEpochStagingDirectory(versionDirectory, epochId), EPOCH_INDEX_SNAPSHOT_FILE_NAME).exists()) {
            return ConsumedEpochStatus.LIVE;
        }
        return ConsumedEpochStatus.STALE;
    }

    private static File latestNumericVersionDirectory(final File keyDirectory) {
        final File[] children = keyDirectory.listFiles(File::isDirectory);
        File latest = null;
        int latestVersion = -1;
        if (children != null) {
            for (final File child : children) {
                try {
                    final int version = Integer.parseInt(child.getName());
                    if (version > latestVersion) {
                        latestVersion = version;
                        latest = child;
                    }
                } catch (final NumberFormatException e) {
                    //non-numeric entries (epoch-staging at key level, leftovers) are not version directories
                }
            }
        }
        return latest;
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
