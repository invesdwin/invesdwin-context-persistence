package de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.filechannel.info.path.FileChannelPath;
import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannel;
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

        if (files != null) {
            for (final File f : files) {
                final String name = f.getName();
                final int underscoreIdx = name.indexOf('_');
                if (underscoreIdx > 0) {
                    try {
                        final int num = Integer.parseInt(name.substring(0, underscoreIdx));
                        if (num >= currentIndexNumber) {
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
    public synchronized void put(final List<MemoryFileSummary> summaries) {
        final MemoryFileMetadata metadata = getMetadata();
        try (ICloseableProperties properties = metadata.getProperties()) {
            for (int i = 0; i < summaries.size(); i++) {
                final MemoryFileSummary summary = summaries.get(i);
                final long precedingMemoryOffset = summary.getPrecedingMemoryOffset();
                final long memoryOffset = summary.getMemoryOffset();
                final long memoryLength = summary.getMemoryLength();
                final File memoryFile = new File(summary.getMemoryResourceUri());
                final long memoryFileSize = precedingMemoryOffset + memoryFile.length();
                final long expectedMemoryFileSize = precedingMemoryOffset + memoryOffset + memoryLength;
                if (memoryFileSize != expectedMemoryFileSize) {
                    throw new IllegalStateException("memoryFileSize[" + memoryFileSize + "] != expectedMemoryFileSize["
                            + expectedMemoryFileSize + "]");
                }
                final long prevMemoryFileSize = metadata.getExpectedMemoryFileSize(properties);
                if (prevMemoryFileSize > expectedMemoryFileSize) {
                    throw new IllegalStateException("memoryFileFize[" + memoryFileSize
                            + "] should not be less than prevMemoryFileSize[" + prevMemoryFileSize + "]");
                }
                metadata.setExpectedMemoryFileSize(properties, expectedMemoryFileSize);
                final V lastValue = parent.getValueSerde().fromBytes(summary.getLastValue());
                final FDate firstValueEndTime = parent
                        .extractEndTime(parent.getValueSerde().fromBytes(summary.getFirstValue()));
                if (!firstValueEndTime.equalsNotNullSafe(summary.getFirstValueEndTime())) {
                    throw new IllegalStateException("summary.firstValue.endTime[" + firstValueEndTime
                            + "] != summary.getFirstValueEndTime[" + summary.getFirstValueEndTime() + "]");
                }
                final FDate lastValueEndTime = parent.extractEndTime(lastValue);
                final int valueCount = summary.getValueCount();
                final long precedingValueCount = summary.getPrecedingValueCount();
                metadata.logSummary(firstValueEndTime, lastValueEndTime, precedingValueCount, valueCount,
                        memoryFile.getAbsolutePath(), precedingMemoryOffset, memoryOffset, memoryLength);
            }
        }

        // Read the index via AMemoryFileSummarySerializingCollection
        final List<MemoryFileSummary> existingSummaries = new ArrayList<>();
        if (latestIndexFile != null && latestIndexFile.exists()) {
            try (IndexSerializingCollection collection = new IndexSerializingCollection(
                    new TextDescription("%s: put: read %s",
                            VersionedTimeSeriesMemoryFileLookupTable.class.getSimpleName(), latestIndexFile),
                    AtomicNioFileChannel.newFile(latestIndexFile.toURI()), true);
                    ICloseableIterator<MemoryFileSummary> it = collection.iterator()) {
                while (it.hasNext()) {
                    existingSummaries.add(it.next());
                }
            }
        }

        for (int i = 0; i < summaries.size(); i++) {
            final MemoryFileSummary newSummary = summaries.get(i);
            if (!existingSummaries.isEmpty()) {
                final MemoryFileSummary lastExisting = existingSummaries.get(existingSummaries.size() - 1);
                final FDate lastFirstEndTime = lastExisting.getFirstValueEndTime();
                final FDate newFirstEndTime = newSummary.getFirstValueEndTime();

                if (lastFirstEndTime != null && lastFirstEndTime.equalsNotNullSafe(newFirstEndTime)) {
                    // Valid replacement for the existing last summary
                    existingSummaries.set(existingSummaries.size() - 1, newSummary);
                } else if (lastFirstEndTime != null && lastFirstEndTime.isAfter(newFirstEndTime)) {
                    // Invalid sequence exception
                    throw new IllegalStateException("existingLastSummary.firstValueEndTime[" + lastFirstEndTime
                            + "] is after firstNewSummary.firstValueEndTime[" + newFirstEndTime + "]");
                } else {
                    // Normal append
                    existingSummaries.add(newSummary);
                }
            } else {
                existingSummaries.add(newSummary);
            }
        }

        // Write the index back to a new file with an incremented number
        currentIndexNumber++;
        final File newIndexFile = new File(directory,
                currentIndexNumber + "_" + AMemoryFileSummarySerializingCollection.MEMORY_INDEX_FILE_NAME);

        try (IndexSerializingCollection collection = new IndexSerializingCollection(
                new TextDescription("%s: put: write %s", VersionedTimeSeriesMemoryFileLookupTable.class.getSimpleName(),
                        newIndexFile),
                AtomicNioFileChannel.newFile(newIndexFile.toURI()), false)) {
            collection.addAll(existingSummaries);
            collection.closeWithEmptyWrite();
        }
        latestIndexFile = newIndexFile;
    }

    @Override
    public synchronized void deleteRange() {
        // Delete all index files
        final File[] files = directory.listFiles(
                (dir, name) -> name.endsWith("_" + AMemoryFileSummarySerializingCollection.MEMORY_INDEX_FILE_NAME));

        if (files != null) {
            for (final File f : files) {
                f.delete();
            }
        }

        latestIndexFile = null;
        currentIndexNumber = 0;
    }

    @Override
    public synchronized ICloseableIterator<MemoryFileSummary> range() {
        // Read index via AMemoryFileSummarySerializingCollection
        if (latestIndexFile != null && latestIndexFile.exists()) {
            final TextDescription name = new TextDescription("%s: put: %s",
                    VersionedTimeSeriesMemoryFileLookupTable.class.getSimpleName());
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