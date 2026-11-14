package de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup;

import java.io.File;
import java.nio.file.FileAlreadyExistsException;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
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
import de.invesdwin.util.error.FastNoSuchElementException;
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

    /**
     * How many delta generations may sit above the chain base before a publish rewrites the chain instead of
     * appending. The rewrite is the expensive step - it costs exactly what every publish costs today - so this bounds
     * it from both sides: at the live worst key (a 2.15 MB base chain, a 329 B compressed publish delta) a rewrite then
     * happens about every 256 publishes, which is ~8 KB of index write per publish against the 4.3 MB today, and no
     * chain read ever opens more than this many files plus the base.
     */
    private static final int CHAIN_MAX_DELTA_GENERATIONS = 256;

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

                //One record of the incoming set is read before anything is written, because whether this publish
                //appends or rewrites depends on it and cannot be revised once a byte of the new generation exists.
                final MemoryFileSummary firstIncoming = nextOrNull(summaries);
                final MemoryFileMetadata metadata = getMetadata();
                final ChainWrite chainWrite;
                try (ICloseableProperties properties = metadata.getProperties()) {
                    final int attestedChainBase = metadata.getChainBase(properties);
                    final FDate attestedChainTail = metadata.getChainTailFirstEndTime(properties);
                    final boolean rewriteChain = mustRewriteChain(attestedChainBase, attestedChainTail, newIndexNumber,
                            firstIncoming);
                    if (!rewriteChain) {
                        assertExtendsChainTail(firstIncoming, attestedChainTail);
                    }
                    chainWrite = writeChain(newCollection, properties, metadata, newIndexNumber, rewriteChain,
                            attestedChainBase, firstIncoming, summaries);
                }

                // Flush the final remaining item to the new file
                if (chainWrite.lastWritten != null) {
                    newCollection.add(chainWrite.lastWritten);
                }

                newCollection.closeWithEmptyWrite();

                //The chain attestation - the base of the contiguous generation range, and the first value end time of
                //the chain's last record - is written AFTER the generation lands, so it can never describe a base that
                //is not on disk. A generation name carries no marker of whether it holds a full snapshot or a delta,
                //so an attested base above the newest present would leave the live range ambiguous, and an ambiguous
                //chain read is a wiped key. The byte high-water keeps its existing place because its pairing with the
                //generation number already makes an interrupted publish recognisable.
                try (ICloseableProperties properties = metadata.getProperties()) {
                    metadata.setChainBase(properties,
                            chainWrite.rewriteChain ? newIndexNumber : chainWrite.attestedChainBase);
                    if (chainWrite.lastWritten != null && chainWrite.lastWritten.getFirstValueEndTime() != null) {
                        metadata.setChainTailFirstEndTime(properties,
                                chainWrite.lastWritten.getFirstValueEndTime());
                    }
                }
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
     * What one publish decided and what it left buffered. The last summary of the new chain is flushed only after the
     * metadata block has closed, so both the buffered record and the append-vs-rewrite decision have to travel out of
     * the write.
     */
    @NotThreadSafe
    private static final class ChainWrite {

        private MemoryFileSummary lastWritten;
        private boolean rewriteChain;
        private int attestedChainBase;
    }

    /**
     * The body of one publish: the existing chain is streamed in ONLY when this publish rewrites, because a delta
     * generation carries just what this publish staged - that is the whole saving. Either way the incoming summaries
     * then go through the same merge, which may replace the last summary of the chain with the first incoming one.
     */
    private ChainWrite writeChain(final IndexSerializingCollection newCollection,
            final ICloseableProperties properties, final MemoryFileMetadata metadata, final int newIndexNumber,
            final boolean rewriteChain, final int attestedChainBase, final MemoryFileSummary firstIncoming,
            final ICloseableIterator<MemoryFileSummary> summaries) {
        final ChainWrite write = new ChainWrite();
        write.attestedChainBase = attestedChainBase;
        write.rewriteChain = rewriteChain;
        //The byte high-water of the existing chain BELOW its last summary: every old summary that is written into the
        //new chain keeps its bytes in the new chain, so this is the floor a new summary may not fall below when it
        //replaces the last summary of the previous generation. Boxed because phase 1 has to hand it to phase 2.
        final long[] retainedChainHighWater = new long[1];
        if (rewriteChain) {
            write.lastWritten = streamExistingChainInto(newCollection, attestedChainBase, retainedChainHighWater);
        }
        write.lastWritten = mergeIncomingSummariesInto(newCollection, properties, metadata, newIndexNumber,
                retainedChainHighWater[0], write.lastWritten, firstIncoming, summaries);
        return write;
    }

    /**
     * Phase 1 of a rewrite: the previous chain, streamed into the new file while keeping the 1-element buffer that
     * makes a replacement of its last record possible. The end of the chain arrives as a
     * {@link NoSuchElementException} from the generation iterator - it closes each generation as the walk passes it -
     * which is why this walks in a try/catch rather than a hasNext loop.
     */
    private MemoryFileSummary streamExistingChainInto(final IndexSerializingCollection newCollection,
            final int attestedChainBase, final long[] retainedChainHighWater) {
        MemoryFileSummary lastWritten = null;
        try (ICloseableIterator<MemoryFileSummary> oldChain = newAttestedChainIterator(attestedChainBase)) {
            while (true) {
                final MemoryFileSummary oldSummary = oldChain.next();
                if (lastWritten != null) {
                    newCollection.add(lastWritten);
                    retainedChainHighWater[0] = lastWritten.getCombinedMemoryFileSize();
                }
                lastWritten = oldSummary;
            }
        } catch (final NoSuchElementException e) {
            // end reached
        }
        return lastWritten;
    }

    /**
     * Phase 2: the summaries this publish staged, merged onto whatever phase 1 left buffered. Returns the still
     * buffered tail of the new chain, which the caller flushes once the metadata block has closed.
     */
    private MemoryFileSummary mergeIncomingSummariesInto(final IndexSerializingCollection newCollection,
            final ICloseableProperties properties, final MemoryFileMetadata metadata, final int newIndexNumber,
            final long retainedChainHighWater, final MemoryFileSummary chainTail,
            final MemoryFileSummary firstIncoming, final ICloseableIterator<MemoryFileSummary> summaries) {
        MemoryFileSummary lastWritten = chainTail;
        MemoryFileSummary newSummary = firstIncoming;
        boolean firstNewSummary = true;
        while (true) {
            if (newSummary == null) {
                break;
            }
            // --- Part A: chain high-water bookkeeping ---
            final long expectedMemoryFileSize = newSummary.getCombinedMemoryFileSize();
            //EXPECTED_MEMORY_FILE_SIZE is the byte high-water of the published generation, not a
            //monotonically growing counter. A new generation may legitimately end BELOW it exactly when its
            //first summary replaces the last summary of the previous generation - the redo of the
            //standalone '_incomplete_' last file, whose last value might be an incomplete bar: that value
            //is dropped, a source that no longer hands it back re-serializes the region shorter than the
            //discarded file was, and the replaced file's bytes leave the chain with the summary that named
            //it. Refusing that fail-loops the updater of the key (every retry redoes the same file,
            //re-produces the same shorter region and dies before publishing): the live signature is
            //'prevMemoryFileSize[4193405] should be less than expectedMemoryFileFize[4193368]' - one bar's
            //worth of compressed bytes. Anything that is NOT such a replacement keeps being measured
            //against the persisted high-water, so a byte space regression stays refused.
            final boolean replacesLastSummary = firstNewSummary && replacesChainTailRecord(lastWritten, newSummary);
            final long prevMemoryFileSize = replacesLastSummary
                    ? retainedChainHighWater
                    : metadata.getExpectedMemoryFileSize(properties);

            if (prevMemoryFileSize > expectedMemoryFileSize) {
                throw new IllegalStateException("prevMemoryFileSize[" + prevMemoryFileSize
                        + "] should be less than expectedMemoryFileFize[" + expectedMemoryFileSize + "]");
            }
            metadata.setExpectedMemoryFileSize(properties, expectedMemoryFileSize, newIndexNumber);

            final FDate firstValueEndTime = parent
                    .extractEndTime(parent.getValueSerde().fromBytes(newSummary.getFirstValue()));

            if (!firstValueEndTime.equalsNotNullSafe(newSummary.getFirstValueEndTime())) {
                throw new IllegalStateException("summary.firstValue.endTime[" + firstValueEndTime
                        + "] != summary.getFirstValueEndTime[" + newSummary.getFirstValueEndTime() + "]");
            }

            // --- Part B: Direct Streaming Merge ---
            lastWritten = mergeNextSummary(newCollection, lastWritten, newSummary, replacesLastSummary);

            firstNewSummary = false;
            newSummary = nextOrNull(summaries);
        }
        return lastWritten;
    }

    /**
     * True when the incoming record names the byte region the chain's buffered tail record already names - the redo of
     * a standalone '_incomplete_' shard, which re-serialises that region into a file of its own.
     */
    private static boolean replacesChainTailRecord(final MemoryFileSummary lastWritten,
            final MemoryFileSummary newSummary) {
        return lastWritten != null && lastWritten.getFirstValueEndTime() != null
                && lastWritten.getFirstValueEndTime().equalsNotNullSafe(newSummary.getFirstValueEndTime());
    }

    /**
     * The merge of one summary onto the buffered chain tail: a replacement discards the buffered record, anything
     * equal-or-earlier is a sequence violation (timestamps must be strictly ascending after a potential replacement),
     * and a continuation writes the buffered record and buffers this one.
     */
    private MemoryFileSummary mergeNextSummary(final IndexSerializingCollection newCollection,
            final MemoryFileSummary lastWritten, final MemoryFileSummary newSummary,
            final boolean replacesLastSummary) {
        if (lastWritten == null) {
            return newSummary;
        }
        if (replacesLastSummary) {
            return newSummary;
        }
        final FDate lastFirstEndTime = lastWritten.getFirstValueEndTime();
        final FDate newFirstEndTime = newSummary.getFirstValueEndTime();
        if (lastFirstEndTime != null
                && (lastFirstEndTime.isAfter(newFirstEndTime) || lastFirstEndTime.equalsNotNullSafe(newFirstEndTime))) {
            throw new IllegalStateException("existingLastSummary.firstValueEndTime[" + lastFirstEndTime
                    + "] does not align with newSummary.firstValueEndTime[" + newFirstEndTime + "]");
        }
        newCollection.add(lastWritten);
        return newSummary;
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
        final int attestedChainBase;
        try (ICloseableProperties properties = getMetadata().getProperties()) {
            attestedChainBase = getMetadata().getChainBase(properties);
        }
        return newAttestedChainIterator(attestedChainBase);
    }

    /**
     * The published chain as one stream: the attested base generation and every generation above it, in number order.
     * Generations are opened one at a time and closed as the walk passes them - each compressed generation carries its
     * own LZ4 block decoder, so opening a whole chain of deltas at once would hold hundreds of megabytes.
     *
     * <p>
     * With no base attested the newest generation is the whole chain, which is how a store still on the cumulative
     * layout reads unchanged until its first rewrite. A hole inside the attested range is damage rather than a shorter
     * chain: the sweep only ever removes generations BELOW the base, so nothing legitimate leaves a gap, and answering
     * a gapped range with what happens to be present would hand a reader a chain whose byte space does not tile.
     */
    private ICloseableIterator<MemoryFileSummary> newAttestedChainIterator(final int attestedChainBase) {
        if (currentIndexFile != null && !currentIndexFile.exists()) {
            moveToNewestGenerationPresent();
        }
        final int newestNumber = currentIndexNumber.get();
        if (newestNumber < 0) {
            return EmptyCloseableIterator.getInstance();
        }
        if (attestedChainBase > newestNumber) {
            throw new IllegalStateException("Chain base [" + attestedChainBase + "] of directory [" + directory
                    + "] is above the newest generation present [" + newestNumber
                    + "] - the base generation itself is gone, so the chain cannot be assembled");
        }
        final int base = attestedChainBase >= 0 ? attestedChainBase : newestNumber;
        final File[] generations = new File[newestNumber - base + 1];
        for (final File f : listIndexGenerations(directory)) {
            final int number = AMemoryFileSummarySerializingCollection.indexNumberOf(f.getName());
            if (number >= base && number <= newestNumber) {
                generations[number - base] = f;
            }
        }
        for (int i = 0; i < generations.length; i++) {
            if (generations[i] == null) {
                throw new IllegalStateException("Index chain of directory [" + directory + "] is missing generation ["
                        + (base + i) + "] of the attested range [" + base + ".." + newestNumber + "] - the sweep only"
                        + " prunes below the base, so a hole inside the chain is damage");
            }
        }
        return new AttestedChainIterator(generations);
    }

    private static MemoryFileSummary nextOrNull(final ICloseableIterator<MemoryFileSummary> summaries) {
        try {
            return summaries.next();
        } catch (final NoSuchElementException e) {
            return null;
        }
    }

    /**
     * Whether this publish has to rewrite the chain instead of appending its records. Three cases force it: no base
     * attested (a key that has never published, or one still carrying the pre-delta cumulative layout, whose newest
     * generation is by itself the whole chain); an incoming first record that REPLACES the chain's last summary (the
     * redo of the standalone '_incomplete_' file names the byte region the current tail already names, so appending it
     * would leave two summaries over one region and every reader-side tiling check would fail); and a delta volume
     * that would make reads open too many files. Rewriting keeps the live chain a CONTIGUOUS generation range, which is
     * what lets a reader ignore superseded generations without ever being told which ones were superseded.
     */
    private static boolean mustRewriteChain(final int attestedChainBase, final FDate attestedChainTail,
            final int newIndexNumber, final MemoryFileSummary firstIncoming) {
        if (attestedChainBase == MemoryFileMetadata.MISSING_CHAIN_BASE || firstIncoming == null) {
            return true;
        }
        if (firstIncoming.getFirstValueEndTime() != null
                && firstIncoming.getFirstValueEndTime().equalsNotNullSafe(attestedChainTail)) {
            return true;
        }
        return newIndexNumber - attestedChainBase >= CHAIN_MAX_DELTA_GENERATIONS;
    }

    /**
     * An appended first record continues the chain: it may not repeat its tail (that is the replacement case, routed to
     * the rewrite by {@link #mustRewriteChain}) and it may not start behind it.
     */
    private static void assertExtendsChainTail(final MemoryFileSummary firstIncoming, final FDate attestedChainTail) {
        if (attestedChainTail == null) {
            return;
        }
        final FDate firstIncomingEndTime = firstIncoming.getFirstValueEndTime();
        if (firstIncomingEndTime == null || !firstIncomingEndTime.isAfter(attestedChainTail)) {
            throw new IllegalStateException("existingChainTail.firstValueEndTime[" + attestedChainTail
                    + "] does not align with newSummary.firstValueEndTime[" + firstIncomingEndTime + "]");
        }
    }

    /**
     * Reads the attested range one generation at a time, closing each before the next is opened.
     */
    private final class AttestedChainIterator implements ICloseableIterator<MemoryFileSummary> {

        private final File[] generations;
        private int index;
        private IndexSerializingCollection openCollection;
        private ICloseableIterator<MemoryFileSummary> openIterator;
        private MemoryFileSummary nextSummary;

        private AttestedChainIterator(final File[] generations) {
            this.generations = generations;
        }

        @Override
        public boolean hasNext() {
            if (nextSummary != null) {
                return true;
            }
            while (openIterator == null || !openIterator.hasNext()) {
                closeCurrent();
                if (index >= generations.length) {
                    return false;
                }
                final File generation = generations[index++];
                openCollection = new IndexSerializingCollection(
                        new TextDescription("%s: range: read %s",
                                VersionedTimeSeriesMemoryFileLookupTable.class.getSimpleName(), generation),
                        AtomicNioFileChannel.newFile(generation.toURI()), true);
                openIterator = openCollection.iterator();
            }
            nextSummary = openIterator.next();
            return true;
        }

        @Override
        public MemoryFileSummary next() {
            if (!hasNext()) {
                throw FastNoSuchElementException.getInstance("end of the attested chain");
            }
            final MemoryFileSummary value = nextSummary;
            nextSummary = null;
            return value;
        }

        @Override
        public void remove() {
            throw FastNoSuchElementException.getInstance("unsupported");
        }

        @Override
        public void close() {
            closeCurrent();
            nextSummary = null;
        }

        private void closeCurrent() {
            if (openIterator != null) {
                openIterator.close();
                openIterator = null;
            }
            if (openCollection != null) {
                openCollection.close();
                openCollection = null;
            }
        }
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