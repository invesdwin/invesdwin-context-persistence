package de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup;

import java.io.File;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.filechannel.info.path.FileChannelPath;
import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannel;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesProperties;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummarySerde;
import de.invesdwin.context.system.properties.ICloseableProperties;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.millis.FDateMillis;

@ThreadSafe
public class VersionedTimeSeriesMemoryFileLookupTable<V> implements ITimeSeriesMemoryFileLookupTable {

    private final TimeSeriesLookupStorageCache<?, V> parent;
    private final File directory;
    private final AtomicNioFileChannel fileChannel;
    private final int version;
    private MemoryFileMetadata memoryFileMetadata;

    private File latestIndexFile;
    private int currentIndexNumber = 0;

    public VersionedTimeSeriesMemoryFileLookupTable(final TimeSeriesLookupStorageCache<?, V> parent, final File file,
            final int version) {
        //CHECKSTYLE:OFF
        this(parent, file.getParentFile(), new AtomicNioFileChannel(FileChannelPath.newFile(file)), version);
        //CHECKSTYLE:ON
    }

    public VersionedTimeSeriesMemoryFileLookupTable(final TimeSeriesLookupStorageCache<?, V> parent,
            final File directory, final AtomicNioFileChannel fileChannel, final int version) {
        this.parent = parent;
        this.directory = directory;
        this.fileChannel = fileChannel;
        this.version = version;

        // Read the latest index defined by the highest number before the actual file name
        final File[] files = directory.listFiles(
                (dir, name) -> name.endsWith("_" + AMemoryFileSummarySerializingCollection.MEMORY_INDEX_FILE_NAME));

        if (files != null && files.length > 0) {
            final long nowMillis = FDateMillis.nowMillis();
            for (int i = 0; i < files.length; i++) {
                final File f = files[i];
                final String name = f.getName();
                final int underscoreIdx = name.indexOf('_');
                if (underscoreIdx > 0) {
                    try {
                        final int num = Integer.parseInt(name.substring(0, underscoreIdx));
                        if (num >= currentIndexNumber) {
                            if (latestIndexFile != null && TimeSeriesProperties.RETAIN_OBSOLETE_FILES_THRESHOLD
                                    .isLessThanMillis(nowMillis - latestIndexFile.lastModified())) {
                                // Delete the older index files
                                latestIndexFile.delete();
                            }
                            currentIndexNumber = num;
                            latestIndexFile = f;
                        }
                    } catch (final NumberFormatException e) {
                        // ignore malformed prefixes
                    }
                }
            }
        }
    }

    public int getVersion() {
        return version;
    }

    @Override
    public synchronized void put(final ICloseableIterator<MemoryFileSummary> summaries) {
        try {
            currentIndexNumber++;
            final File newIndexFile = new File(directory,
                    currentIndexNumber + "_" + AMemoryFileSummarySerializingCollection.MEMORY_INDEX_FILE_NAME);

            fileChannel.setFileName(newIndexFile.getName());

            try (IndexSerializingCollection newCollection = new IndexSerializingCollection(
                    new TextDescription("%s: put: write %s",
                            VersionedTimeSeriesMemoryFileLookupTable.class.getSimpleName(), newIndexFile),
                    fileChannel, false)) {

                // Buffer exactly one element to allow replacement of the final element if required
                MemoryFileSummary lastWritten = null;

                // 1. Stream existing index elements to the new file, maintaining the 1-element buffer
                if (latestIndexFile != null && latestIndexFile.exists()) {
                    try (IndexSerializingCollection oldCollection = new IndexSerializingCollection(
                            new TextDescription("%s: put: read %s",
                                    VersionedTimeSeriesMemoryFileLookupTable.class.getSimpleName(), latestIndexFile),
                            AtomicNioFileChannel.newFile(latestIndexFile.toURI()), true);
                            ICloseableIterator<MemoryFileSummary> oldIt = oldCollection.iterator()) {
                        while (true) {
                            final MemoryFileSummary oldSummary = oldIt.next();
                            if (lastWritten != null) {
                                newCollection.add(lastWritten);
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
                        final long expectedMemoryFileSize = precedingMemoryOffset + memoryOffset + memoryLength;
                        final long prevMemoryFileSize = metadata.getExpectedMemoryFileSize(properties);

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

                            if (firstNewSummary && lastFirstEndTime != null
                                    && lastFirstEndTime.equalsNotNullSafe(newFirstEndTime)) {
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

            latestIndexFile = newIndexFile;
        } finally {
            summaries.close();
        }
    }

    @Override
    public synchronized ICloseableIterator<MemoryFileSummary> range() {
        // Read index via AMemoryFileSummarySerializingCollection
        if (latestIndexFile != null && latestIndexFile.exists()) {
            final IndexSerializingCollection collection = new IndexSerializingCollection(
                    new TextDescription("%s: range: read %s",
                            VersionedTimeSeriesMemoryFileLookupTable.class.getSimpleName(), latestIndexFile),
                    AtomicNioFileChannel.newFile(latestIndexFile.toURI()), true);
            return collection.iterator();
        }
        return EmptyCloseableIterator.getInstance();
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
}