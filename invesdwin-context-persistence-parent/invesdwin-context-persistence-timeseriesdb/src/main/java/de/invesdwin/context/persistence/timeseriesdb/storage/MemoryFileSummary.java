package de.invesdwin.context.persistence.timeseriesdb.storage;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.norva.marker.ISerializableValueObject;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.file.IMemoryMappedFile;

@Immutable
public class MemoryFileSummary implements ISerializableValueObject {

    private final byte[] firstValue;
    private final byte[] lastValue;
    private final long precedingValueCount;
    private final int valueCount;
    private final String memoryResourceUri;
    private final long precedingMemoryOffset;
    private final long memoryOffset;
    private final long memoryLength;
    private final int hashCode;

    public <V> MemoryFileSummary(final byte[] firstValue, final byte[] lastValue, final long precedingValueCount,
            final int valueCount, final String memoryResourceUri, final long precedingMemoryOffset,
            final long memoryOffset, final long memoryLength) {
        this.firstValue = firstValue;
        this.lastValue = lastValue;
        this.precedingValueCount = precedingValueCount;
        this.valueCount = valueCount;
        this.memoryResourceUri = memoryResourceUri;
        this.precedingMemoryOffset = precedingMemoryOffset;
        this.memoryOffset = memoryOffset;
        this.memoryLength = memoryLength;
        this.hashCode = newHashCode();
    }

    public <V> MemoryFileSummary(final ISerde<V> serde, final V firstValue, final V lastValue,
            final long precedingValueCount, final int valueCount, final String memoryResourceUri,
            final long precedingMemoryOffset, final long memoryOffset, final long memoryLength) {
        this.firstValue = serde.toBytes(firstValue);
        this.lastValue = serde.toBytes(lastValue);
        this.precedingValueCount = precedingValueCount;
        this.valueCount = valueCount;
        this.memoryResourceUri = memoryResourceUri;
        this.precedingMemoryOffset = precedingMemoryOffset;
        this.memoryOffset = memoryOffset;
        this.memoryLength = memoryLength;
        this.hashCode = newHashCode();
    }

    private int newHashCode() {
        return Objects.hashCode(memoryResourceUri, precedingMemoryOffset, memoryOffset, memoryLength);
    }

    public <V> V getFirstValue(final ISerde<V> serde) {
        return serde.fromBytes(firstValue);
    }

    public byte[] getFirstValue() {
        return firstValue;
    }

    public <V> V getLastValue(final ISerde<V> serde) {
        return serde.fromBytes(lastValue);
    }

    public byte[] getLastValue() {
        return lastValue;
    }

    public long getPrecedingValueCount() {
        return precedingValueCount;
    }

    public int getValueCount() {
        return valueCount;
    }

    public long getCombinedValueCount() {
        return precedingValueCount + valueCount;
    }

    public String getMemoryResourceUri() {
        return memoryResourceUri;
    }

    public long getPrecedingMemoryOffset() {
        return precedingMemoryOffset;
    }

    public long getMemoryOffset() {
        return memoryOffset;
    }

    public long getMemoryLength() {
        return memoryLength;
    }

    public IByteBuffer newBuffer(final IMemoryMappedFile file) {
        final int length = Integers.checkedCast(getMemoryLength());
        return file.newByteBuffer(getMemoryOffset(), length);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof MemoryFileSummary) {
            final MemoryFileSummary cObj = (MemoryFileSummary) obj;
            return Objects.equals(cObj.memoryResourceUri, memoryResourceUri)
                    && cObj.precedingMemoryOffset == precedingMemoryOffset && cObj.memoryOffset == memoryOffset
                    && cObj.memoryLength == memoryLength;
        }
        return false;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("uri", memoryResourceUri)
                .add("precedingOffset", precedingMemoryOffset)
                .add("offset", memoryOffset)
                .add("length", memoryLength)
                .toString();
    }

}