package de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup;

import java.io.File;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.filechannel.info.path.FileChannelPath;
import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannel;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.util.collections.iterable.ICloseableIterator;

@ThreadSafe
public class VersionedTimeSeriesMemoryFileLookupTable implements ITimeSeriesMemoryFileLookupTable {

    private final File directory;
    private final AtomicNioFileChannel fileChannel;
    private final int version;
    private MemoryFileMetadata memoryFileMetadata;

    public VersionedTimeSeriesMemoryFileLookupTable(final File file, final int version) {
        //CHECKSTYLE:OFF
        this(file.getParentFile(), new AtomicNioFileChannel(
                FileChannelPath.valueOfFile(file.toURI(), AtomicNioFileChannel.DEFAULT_SERVER_URI_F)), version);
        //CHECKSTYLE:ON
    }

    public VersionedTimeSeriesMemoryFileLookupTable(final File directory, final AtomicNioFileChannel fileChannel,
            final int version) {
        this.directory = directory;
        this.fileChannel = fileChannel;
        this.version = version;
    }

    public int getVersion() {
        return version;
    }

    @Override
    public void put(final MemoryFileSummary summary) {}

    @Override
    public void deleteRange() {}

    @Override
    public ICloseableIterator<MemoryFileSummary> range() {
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
