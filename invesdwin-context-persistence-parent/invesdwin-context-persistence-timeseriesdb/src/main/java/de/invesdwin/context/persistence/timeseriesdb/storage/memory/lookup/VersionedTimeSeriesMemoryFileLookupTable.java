package de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup;

import java.io.File;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.filechannel.info.path.FileChannelPath;
import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannel;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.system.properties.ICloseableProperties;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.time.date.FDate;

@ThreadSafe
public class VersionedTimeSeriesMemoryFileLookupTable<V> implements ITimeSeriesMemoryFileLookupTable {

    private final TimeSeriesLookupStorageCache<?, V> parent;
    private final File directory;
    private final AtomicNioFileChannel fileChannel;
    private final int version;
    private MemoryFileMetadata memoryFileMetadata;

    public VersionedTimeSeriesMemoryFileLookupTable(final TimeSeriesLookupStorageCache<?, V> parent, final File file,
            final int version) {
        //CHECKSTYLE:OFF
        this(parent, file.getParentFile(), new AtomicNioFileChannel(
                FileChannelPath.valueOfFile(file.toURI(), AtomicNioFileChannel.DEFAULT_SERVER_URI_F)), version);
        //CHECKSTYLE:ON
    }

    public VersionedTimeSeriesMemoryFileLookupTable(final TimeSeriesLookupStorageCache<?, V> parent,
            final File directory, final AtomicNioFileChannel fileChannel, final int version) {
        this.parent = parent;
        this.directory = directory;
        this.fileChannel = fileChannel;
        this.version = version;
        /*
         * TODO: at the start read the latest (defined by the highest number before the actual file name) index; when
         * writing the index, write it to a new file with a higher number.
         */
    }

    public int getVersion() {
        return version;
    }

    @Override
    public void put(final List<MemoryFileSummary> summaries) {
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
                /*
                 * TODO: read the index (in fileChannel) via AMemoryFileSummarySerializingCollection and append the new
                 * summaries, then write the index back to fileChannel. Though if an existing last summary is already in
                 * the index, it should be replaced with the first new summary (based on summary.firstValueEndTime). If
                 * the firstValueEndTime of a replaced summary is not equal, then exception should be thrown (e.g.
                 * existingLastSummary.firstValueEndTime is after firstNewSummary.firstValueEndTime). Any other
                 * replacements should be illegal.
                 */
            }
        }
    }

    @Override
    public void deleteRange() {
        //TODO: delete the index (in fileChannel)
    }

    @Override
    public ICloseableIterator<MemoryFileSummary> range() {
        //  System.out.println("TODO: read index via AMemoryFileSummarySerializingCollection (in fileChannel)");
        return null;
    }

    @Override
    public MemoryFileMetadata getMetadata() {
        if (memoryFileMetadata == null) {
            synchronized (this) {
                memoryFileMetadata = new MemoryFileMetadata(directory);
            }
        }
        return memoryFileMetadata;
    }

}
