package de.invesdwin.context.persistence.timeseriesdb;

import java.util.List;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.date.FDate;

@Immutable
public class PrepareForUpdateResult<V> {

    private final FDate updateFrom;
    private final List<V> lastValues;
    private final long precedingMemorOffset;
    private final long memoryOffset;
    private final long precedingValueCount;

    public PrepareForUpdateResult(final FDate updateFrom, final List<V> lastValues, final long precedingMemoryOffset,
            final long memoryOffset, final long precedingValueCount) {
        this.updateFrom = updateFrom;
        Assertions.checkNotNull(lastValues);
        this.lastValues = lastValues;
        this.precedingMemorOffset = precedingMemoryOffset;
        this.memoryOffset = memoryOffset;
        this.precedingValueCount = precedingValueCount;
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

}
