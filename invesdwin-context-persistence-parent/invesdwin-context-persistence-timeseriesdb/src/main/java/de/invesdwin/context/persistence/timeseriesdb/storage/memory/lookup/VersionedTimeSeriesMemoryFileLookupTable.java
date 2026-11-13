package de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup;

import java.io.File;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.filechannel.info.path.FileChannelPath;
import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannel;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummarySerde;
import de.invesdwin.context.system.properties.ICloseableProperties;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.time.date.FDate;

@ThreadSafe
public class VersionedTimeSeriesMemoryFileLookupTable<V> implements ITimeSeriesMemoryFileLookupTable {

    private static final Log LOG = new Log(VersionedTimeSeriesMemoryFileLookupTable.class);

    private final TimeSeriesLookupStorageCache<?, V> parent;
    private final File directory;
    private final AtomicNioFileChannel baseChannel;
    private final int version;
    private MemoryFileMetadata memoryFileMetadata;

    @GuardedBy("this")
    private File currentIndexFile;
    private final AtomicInteger currentIndexNumber = new AtomicInteger(-1);

    public VersionedTimeSeriesMemoryFileLookupTable(final TimeSeriesLookupStorageCache<?, V> parent,
            final File directory, final int version) {
        this.parent = parent;
        this.directory = directory;
        this.baseChannel = new AtomicNioFileChannel(FileChannelPath.newDirectory(directory));
        this.version = version;

        // Read the latest index defined by the highest number before the actual file name.
        // Nothing is deleted here: this constructor sits on the read path of every cache miss, and a generation that
        // another live instance of this key still resolves is that instance's view of the store - removing it here
        // made the instance answer an empty chain, which is what a re-resolving reader does to a store the trading
        // side then reports as inconsistent. Generation deletion lives in TimeSeriesEpochReclaim, which proves the
        // retained set covers what a deleted generation names before it removes anything.
        final File[] files = listIndexGenerations(directory);
        for (final File f : files) {
            final int num = AMemoryFileSummarySerializingCollection.indexNumberOf(f.getName());
            if (num > currentIndexNumber.get()) {
                currentIndexNumber.set(num);
                currentIndexFile = f;
            }
        }
    }

    /**
     * The index generations of a key's version data directory, every one of them named
     * {@code <N>_memory.index}. One listing helper for the three places that need it (this table's constructor, its
     * update probe, and the reclaim sweep), because a directory with ~1,500 generations makes an unfiltered listing
     * and a second parse pass expensive.
     */
    public static File[] listIndexGenerations(final File directory) {
        final File[] files = directory
                .listFiles((dir, name) -> AMemoryFileSummarySerializingCollection.isIndexGenerationFileName(name));
        return files != null ? files : new File[0];
    }

    public int getVersion() {
        return version;
    }

    @Override
    public synchronized void put(final ICloseableIterator<MemoryFileSummary> summaries) {
        try {
            final int newIndexNumber = currentIndexNumber.get() + 1;
            final File newIndexFile = new File(directory,
                    AMemoryFileSummarySerializingCollection.newIndexGenerationFileName(newIndexNumber));

            baseChannel.setFileName(newIndexFile.getName());

            try (IndexSerializingCollection newCollection = new IndexSerializingCollection(
                    new TextDescription("%s: put: write %s",
                            VersionedTimeSeriesMemoryFileLookupTable.class.getSimpleName(), newIndexFile),
                    baseChannel, false)) {

                // Buffer exactly one element to allow replacement of the final element if required
                MemoryFileSummary lastWritten = null;
                //The byte high-water of the existing chain BELOW its last summary: every old summary that is written
                //into the new index keeps its bytes in the new chain, so this is the floor a new summary may not fall
                //below when it replaces the last summary of the previous generation.
                long retainedChainHighWater = 0L;

                // 1. Stream existing index elements to the new file, maintaining the 1-element buffer
                if (currentIndexFile != null && currentIndexFile.exists()) {
                    try (IndexSerializingCollection oldCollection = new IndexSerializingCollection(
                            new TextDescription("%s: put: read %s",
                                    VersionedTimeSeriesMemoryFileLookupTable.class.getSimpleName(), currentIndexFile),
                            AtomicNioFileChannel.newFile(currentIndexFile.toURI()), true);
                            ICloseableIterator<MemoryFileSummary> oldIt = oldCollection.iterator()) {
                        while (true) {
                            final MemoryFileSummary oldSummary = oldIt.next();
                            if (lastWritten != null) {
                                newCollection.add(lastWritten);
                                retainedChainHighWater = lastWritten.getCombinedMemoryFileSize();
                            }
                            lastWritten = oldSummary;
                        }
                    } catch (final NoSuchElementException e) {
                        // end reached
                    }
                }

                // 2. Stream incoming summaries, log metadata, and directly merge/append via the buffer
                boolean firstNewSummary = true;
                final MemoryFileMetadata metadata = getMetadata();
                try (ICloseableProperties properties = metadata.getProperties()) {
                    while (true) {
                        final MemoryFileSummary newSummary = summaries.next();

                        // --- Part A: Metadata Logging ---
                        final long precedingMemoryOffset = newSummary.getPrecedingMemoryOffset();
                        final long memoryOffset = newSummary.getMemoryOffset();
                        final long memoryLength = newSummary.getMemoryLength();
                        final long expectedMemoryFileSize = newSummary.getCombinedMemoryFileSize();
                        //EXPECTED_MEMORY_FILE_SIZE is the byte high-water of the published generation, not a
                        //monotonically growing counter. A new generation may legitimately end BELOW it exactly when its
                        //first summary replaces the last summary of the previous generation - the redo of the
                        //standalone '_incomplete_' last file, whose last value might be an incomplete bar: that value
                        //is dropped, a source that no longer hands it back re-serializes the region shorter than the
                        //discarded file was, and the replaced file's bytes leave the chain with the summary that named
                        //it. Refusing that shrink fail-loops the updater of the key (every retry redoes the same file,
                        //re-produces the same shorter region and dies before publishing): the live signature is
                        //'prevMemoryFileSize[4193405] should be less than expectedMemoryFileFize[4193368]' - one bar's
                        //worth of compressed bytes. Anything that is NOT such a replacement keeps being measured
                        //against the persisted high-water, so a byte space regression stays refused.
                        final boolean replacesLastSummary = firstNewSummary && lastWritten != null
                                && lastWritten.getFirstValueEndTime() != null
                                && lastWritten.getFirstValueEndTime()
                                        .equalsNotNullSafe(newSummary.getFirstValueEndTime());
                        final long prevMemoryFileSize = replacesLastSummary
                                ? retainedChainHighWater
                                : metadata.getExpectedMemoryFileSize(properties);

                        if (prevMemoryFileSize > expectedMemoryFileSize) {
                            throw new IllegalStateException("prevMemoryFileSize[" + prevMemoryFileSize
                                    + "] should be less than expectedMemoryFileFize[" + expectedMemoryFileSize + "]");
                        }
                        metadata.setExpectedMemoryFileSize(properties, expectedMemoryFileSize);

                        final V lastValue = parent.getValueSerde().fromBytes(newSummary.getLastValue());
                        final FDate firstValueEndTime = parent
                                .extractEndTime(parent.getValueSerde().fromBytes(newSummary.getFirstValue()));

                        if (!firstValueEndTime.equalsNotNullSafe(newSummary.getFirstValueEndTime())) {
                            throw new IllegalStateException("summary.firstValue.endTime[" + firstValueEndTime
                                    + "] != summary.getFirstValueEndTime[" + newSummary.getFirstValueEndTime() + "]");
                        }

                        final FDate lastValueEndTime = parent.extractEndTime(lastValue);
                        final long precedingValueCount = newSummary.getPrecedingValueCount();
                        final int valueCount = newSummary.getValueCount();
                        final String memoryResourceUri = newSummary.getMemoryResourceUri();

                        metadata.logSummary(firstValueEndTime, lastValueEndTime, precedingValueCount, valueCount,
                                memoryResourceUri, precedingMemoryOffset, memoryOffset, memoryLength);

                        // --- Part B: Direct Streaming Merge ---
                        if (lastWritten != null) {
                            final FDate lastFirstEndTime = lastWritten.getFirstValueEndTime();
                            final FDate newFirstEndTime = newSummary.getFirstValueEndTime();

                            if (replacesLastSummary) {
                                // Valid single replacement: discard lastWritten (last from old) and buffer newSummary (first from iterator)
                                lastWritten = newSummary;
                            } else if (lastFirstEndTime != null && (lastFirstEndTime.isAfter(newFirstEndTime)
                                    || lastFirstEndTime.equalsNotNullSafe(newFirstEndTime))) {
                                // Invalid sequence exception: timestamps must be strictly ascending after potential replacement
                                throw new IllegalStateException("existingLastSummary.firstValueEndTime["
                                        + lastFirstEndTime + "] does not align with newSummary.firstValueEndTime["
                                        + newFirstEndTime + "]");
                            } else {
                                // Normal append: write the buffered item, and buffer the newSummary
                                newCollection.add(lastWritten);
                                lastWritten = newSummary;
                            }
                        } else {
                            lastWritten = newSummary;
                        }

                        firstNewSummary = false;
                    }
                } catch (final NoSuchElementException e) {
                    // end reached
                }

                // Flush the final remaining item to the new file
                if (lastWritten != null) {
                    newCollection.add(lastWritten);
                }

                newCollection.closeWithEmptyWrite();
            }

            currentIndexFile = newIndexFile;
            currentIndexNumber.set(newIndexNumber);
        } finally {
            summaries.close();
        }
    }

    @Override
    public synchronized ICloseableIterator<MemoryFileSummary> range() {
        // Read index via AMemoryFileSummarySerializingCollection
        if (currentIndexFile != null && !currentIndexFile.exists()) {
            moveToNewestGenerationPresent();
        }
        if (currentIndexFile != null && currentIndexFile.exists()) {
            final IndexSerializingCollection collection = new IndexSerializingCollection(
                    new TextDescription("%s: range: read %s",
                            VersionedTimeSeriesMemoryFileLookupTable.class.getSimpleName(), currentIndexFile),
                    AtomicNioFileChannel.newFile(currentIndexFile.toURI()), true);
            return collection.iterator();
        }
        return EmptyCloseableIterator.getInstance();
    }

    /**
     * A generation this instance resolved can be reclaimed while it lives: the reclaim runs on another thread, and on
     * shared storage another process's reclaim can outrun this one's notion of what is current. Re-resolving the newest
     * generation present is the recovery, and it is a strictly better answer than the empty chain this method used to
     * return - an empty chain reads as "the store holds nothing", which the trading derived-store guards take as
     * licence to delete and rebuild the key.
     */
    private void moveToNewestGenerationPresent() {
        final File lostGeneration = currentIndexFile;
        File newest = null;
        int newestNumber = Integer.MIN_VALUE;
        for (final File f : listIndexGenerations(directory)) {
            final int num = AMemoryFileSummarySerializingCollection.indexNumberOf(f.getName());
            if (num > newestNumber) {
                newestNumber = num;
                newest = f;
            }
        }
        LOG.warn("Index generation [%s] of directory [%s] is gone - this table moves to the newest generation present [%s]",
                lostGeneration.getName(), directory, newest != null ? newest.getName() : "none");
        currentIndexFile = newest;
        if (newest != null) {
            currentIndexNumber.set(newestNumber);
        }
    }

    @Override
    public MemoryFileMetadata getMetadata() {
        if (memoryFileMetadata == null) {
            synchronized (this) {
                if (memoryFileMetadata == null) {
                    memoryFileMetadata = new MemoryFileMetadata(directory);
                }
            }
        }
        return memoryFileMetadata;
    }

    /**
     * Inner utility class to instantiate the required SerializingCollection logic for index files.
     */
    private final class IndexSerializingCollection extends AMemoryFileSummarySerializingCollection {

        private IndexSerializingCollection(final TextDescription name, final AtomicNioFileChannel fileChannel,
                final boolean readOnly) {
            super(name, fileChannel, readOnly);
        }

        @Override
        protected MemoryFileSummarySerde newSerde() {
            return new MemoryFileSummarySerde(parent.getValueFixedLength());
        }

        @Override
        protected ICompressionFactory getCompressionFactory() {
            return parent.getCompressionFactory();
        }
    }

    @Override
    public boolean isUpdatedIndexAvailable() {
        final int currentNumber = currentIndexNumber.get();
        for (final File f : listIndexGenerations(directory)) {
            if (AMemoryFileSummarySerializingCollection.indexNumberOf(f.getName()) > currentNumber) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int getCurrentIndexNumber() {
        return currentIndexNumber.get();
    }
}