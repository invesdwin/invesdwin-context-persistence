package de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup;

import java.io.File;
import java.nio.file.FileAlreadyExistsException;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.filechannel.info.path.FileChannelPath;
import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannel;
import de.invesdwin.util.concurrent.lock.file.AtomicNioFileChannelContext;
import de.invesdwin.util.error.Throwables;
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

    /**
     * How often one publish may re-resolve its generation number before it gives up. Every retry costs one directory
     * listing, so this bounds the worst case rather than the common one: the number is free on the first pass unless
     * another writer of this key is publishing at the same moment.
     */
    private static final int GENERATION_CLAIM_ATTEMPTS = 8;

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
        //Claim mode, not replace mode: a generation name belongs to one writer. Replace mode here is what let two
        //instances of one key publish the same '&lt;N&gt;_memory.index' and have the later rename delete the earlier chain.
        this.baseChannel = new AtomicNioFileChannel(FileChannelPath.newDirectory(directory),
                new AtomicNioFileChannelContext(true));
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
            final File newIndexFile = claimNextGenerationFile();
            final int newIndexNumber = AMemoryFileSummarySerializingCollection.indexNumberOf(newIndexFile.getName());

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

                // 2. Stream incoming summaries, track the chain high-water, and directly merge/append via the buffer
                boolean firstNewSummary = true;
                int publishedSummaryCount = 0;
                long publishedChainHighWater = 0L;
                final MemoryFileMetadata metadata = getMetadata();
                try (ICloseableProperties properties = metadata.getProperties()) {
                    while (true) {
                        final MemoryFileSummary newSummary = summaries.next();

                        // --- Part A: chain high-water bookkeeping ---
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
                        publishedSummaryCount++;
                        publishedChainHighWater = expectedMemoryFileSize;

                        final FDate firstValueEndTime = parent
                                .extractEndTime(parent.getValueSerde().fromBytes(newSummary.getFirstValue()));

                        if (!firstValueEndTime.equalsNotNullSafe(newSummary.getFirstValueEndTime())) {
                            throw new IllegalStateException("summary.firstValue.endTime[" + firstValueEndTime
                                    + "] != summary.getFirstValueEndTime[" + newSummary.getFirstValueEndTime() + "]");
                        }

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
                    //One record for the whole publish, after the generation's head is known - see
                    //MemoryFileMetadata.logPublish for why it is not one per summary.
                    metadata.logPublish(publishedSummaryCount, lastWritten, publishedChainHighWater, parent
                            .extractEndTime(parent.getValueSerde().fromBytes(lastWritten.getLastValue())));
                }

                newCollection.closeWithEmptyWrite();
            }

            currentIndexFile = newIndexFile;
            currentIndexNumber.set(newIndexNumber);
        } catch (final RuntimeException e) {
            final FileAlreadyExistsException claimRefused = Throwables.getCauseByType(e,
                    FileAlreadyExistsException.class);
            if (claimRefused != null) {
                //The listing said the name was free and the publish said it was taken: a second writer of this key won
                //the window. This cannot be retried in place - the summaries iterator is already drained - but the
                //updater's own retry re-runs the pass against a fresh listing, so the cost is one wasted pass and no
                //lost generation, whereas in replace mode the same race silently deleted the other writer's chain.
                throw new IllegalStateException("Index generation [" + baseChannel.getFileName() + "] of directory ["
                        + directory + "] was claimed by another writer between the listing and the publish - this key"
                        + " is being published by more than one process while the update claim that should exclude them"
                        + " is held or stale", claimRefused);
            }
            throw e;
        } finally {
            summaries.close();
        }
    }

    /**
     * The generation file this publish writes into. The number is re-read from the directory instead of taken from the
     * counter this instance latched, because the counter is per instance: two holders of one key's update claim exist
     * whenever a heartbeat claim goes stale on a share (the very case the epoch reclaim's liveness guard exists for),
     * and both would then pick the same '&lt;N&gt;_memory.index'. The channel publishes by rename, and a rename
     * REPLACES an existing target, so the earlier writer's chain would simply disappear - no exception, and no reader
     * can tell that the summaries below the high-water are gone. Re-resolving narrows that window to one directory
     * listing and turns a collision into a retry; a target still occupied after {@link #GENERATION_CLAIM_ATTEMPTS} is
     * a hard error rather than a silent overwrite, because what is supposed to keep one writer per key is the heartbeat
     * update claim, not the file name.
     */
    private File claimNextGenerationFile() {
        for (int attempt = 0; attempt < GENERATION_CLAIM_ATTEMPTS; attempt++) {
            int newestNumber = currentIndexNumber.get();
            for (final File existing : listIndexGenerations(directory)) {
                final int number = AMemoryFileSummarySerializingCollection.indexNumberOf(existing.getName());
                if (number > newestNumber) {
                    newestNumber = number;
                }
            }
            final File candidate = new File(directory,
                    AMemoryFileSummarySerializingCollection.newIndexGenerationFileName(newestNumber + 1));
            if (!candidate.exists()) {
                return candidate;
            }
            LOG.warn("Index generation [%s] of directory [%s] already exists - another writer claimed it,"
                    + " re-resolving (attempt %s of %s)", candidate.getName(), directory, attempt + 1,
                    GENERATION_CLAIM_ATTEMPTS);
        }
        throw new IllegalStateException("Cannot claim a free index generation in [" + directory + "] after ["
                + GENERATION_CLAIM_ATTEMPTS + "] attempts - this key is being published by more than one writer while"
                + " the update claim that should exclude them is held or stale");
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