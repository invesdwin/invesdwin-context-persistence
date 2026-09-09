package de.invesdwin.context.persistence.timeseriesdb;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.ITimeSeriesMemoryFileLookupTable;
import de.invesdwin.util.assertions.Assertions;
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
    private List<MemoryFileSummary> summaries;

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
        assertSummaryBeforeCommit(summary);
        if (summaries == null) {
            summaries = new ArrayList<>();
        }
        summaries.add(summary);
    }

    private void assertSummaryBeforeCommit(final MemoryFileSummary summary) {
        final MemoryFileSummary lastSummary;
        if (summaries.isEmpty()) {
            lastSummary = parent.getLastRangeKey();
        } else {
            lastSummary = summaries.get(summaries.size() - 1);
        }
        parent.assertSummary(lastSummary, summary);
    }

    @Override
    public void close() {
        if (summaries == null || summaries.isEmpty()) {
            return;
        }
        final ITimeSeriesMemoryFileLookupTable memoryFileLookupTable = parent.getMemoryFileLookupTable();
        memoryFileLookupTable.put(summaries);
        summaries.clear();
        parent.clearCaches();
    }

}
