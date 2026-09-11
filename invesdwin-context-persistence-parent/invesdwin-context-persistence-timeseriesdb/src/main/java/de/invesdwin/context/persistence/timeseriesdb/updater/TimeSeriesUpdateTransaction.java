package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannel;
import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannelContext;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummarySerde;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.AMemoryFileSummarySerializingCollection;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.ITimeSeriesMemoryFileLookupTable;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.streams.closeable.ISafeCloseable;
import de.invesdwin.util.time.date.FDate;

@Immutable
public class TimeSeriesUpdateTransaction<V> implements ISafeCloseable {

    private final TimeSeriesLookupStorageCache<?, V> parent;
    private final FDate updateFrom;
    private final List<V> lastValues;
    private final long precedingMemorOffset;
    private final long memoryOffset;
    private final long precedingValueCount;
    private AMemoryFileSummarySerializingCollection summaries;
    private MemoryFileSummary prevSummary;

    public TimeSeriesUpdateTransaction(final TimeSeriesLookupStorageCache<?, V> parent, final FDate updateFrom,
            final List<V> lastValues, final long precedingMemoryOffset, final long memoryOffset,
            final long precedingValueCount) {
        this.parent = parent;
        this.updateFrom = updateFrom;
        Assertions.checkNotNull(lastValues);
        this.lastValues = lastValues;
        this.precedingMemorOffset = precedingMemoryOffset;
        this.memoryOffset = memoryOffset;
        this.precedingValueCount = precedingValueCount;
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

    public long getPrecedingMemorOffset() {
        return precedingMemorOffset;
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
        assertSummaryBeforeCommit(summary);
        summaries.add(summary);
        prevSummary = summary;
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

    @Override
    public void close() {
        if (prevSummary == null) {
            return;
        }
        summaries.closeWithEmptyWrite();
        final ITimeSeriesMemoryFileLookupTable memoryFileLookupTable = parent.getMemoryFileLookupTable();
        memoryFileLookupTable.put(summaries.iterator());
        summaries.clear();
        parent.clearCaches();
    }

}
