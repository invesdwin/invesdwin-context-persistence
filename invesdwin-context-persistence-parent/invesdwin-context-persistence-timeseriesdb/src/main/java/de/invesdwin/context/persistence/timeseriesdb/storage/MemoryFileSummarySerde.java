package de.invesdwin.context.persistence.timeseriesdb.storage;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.SerdeBaseMethods;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@NotThreadSafe
public final class MemoryFileSummarySerde implements ISerde<MemoryFileSummary> {

    private static final int VALUECOUNT_INDEX = 0;
    private static final int VALUECOUNT_SIZE = Integer.BYTES;

    private static final int MEMORYRESOURCEURISIZE_INDEX = VALUECOUNT_INDEX + VALUECOUNT_SIZE;
    private static final int MEMORYRESOURCEURISIZE_SIZE = Integer.BYTES;

    private static final int MEMORYOFFSET_INDEX = MEMORYRESOURCEURISIZE_INDEX + MEMORYRESOURCEURISIZE_SIZE;
    private static final int MEMORYOFFSET_SIZE = Long.BYTES;

    private static final int MEMORYLENGTH_INDEX = MEMORYOFFSET_INDEX + MEMORYOFFSET_SIZE;
    private static final int MEMORYLENGTH_SIZE = Long.BYTES;

    private static final int VALUELENGTH_SIZE = Integer.BYTES;

    private final int firstValueLengthIndex;
    private final int lastValueLengthIndex;
    private final int firstValueIndex;
    private final int lastValueIndex;
    private final int memoryResourceUriIndex;

    private final Integer valueFixedLength;

    public MemoryFileSummarySerde(final Integer valueFixedLength) {
        this.valueFixedLength = valueFixedLength;
        if (valueFixedLength != null) {
            firstValueLengthIndex = -1;
            lastValueLengthIndex = -1;
            firstValueIndex = MEMORYLENGTH_INDEX + MEMORYLENGTH_SIZE;
            lastValueIndex = firstValueIndex + valueFixedLength;
            memoryResourceUriIndex = lastValueIndex + valueFixedLength;
        } else {
            firstValueLengthIndex = MEMORYLENGTH_INDEX + MEMORYLENGTH_SIZE;
            lastValueLengthIndex = firstValueLengthIndex + VALUELENGTH_SIZE;
            firstValueIndex = lastValueLengthIndex + VALUELENGTH_SIZE;
            lastValueIndex = -1;
            memoryResourceUriIndex = -1;
        }
    }

    @Override
    public MemoryFileSummary fromBytes(final byte[] bytes) {
        return SerdeBaseMethods.fromBytes(this, bytes);
    }

    @Override
    public byte[] toBytes(final MemoryFileSummary obj) {
        return SerdeBaseMethods.toBytes(this, obj);
    }

    @Override
    public MemoryFileSummary fromBuffer(final IByteBuffer buffer) {
        final int valueCount = buffer.getInt(VALUECOUNT_INDEX);
        final int memoryResourceUriSize = buffer.getInt(MEMORYRESOURCEURISIZE_INDEX);
        final long memoryOffset = buffer.getLong(MEMORYOFFSET_INDEX);
        final long memoryLength = buffer.getLong(MEMORYLENGTH_INDEX);
        if (valueFixedLength != null) {
            final byte[] firstValue = ByteBuffers.allocateByteArray(valueFixedLength);
            buffer.getBytes(firstValueIndex, firstValue);
            final byte[] lastValue = ByteBuffers.allocateByteArray(valueFixedLength);
            buffer.getBytes(lastValueIndex, lastValue);
            final String memoryResourceUri = buffer.getStringUtf8(memoryResourceUriIndex, memoryResourceUriSize);
            return new MemoryFileSummary(firstValue, lastValue, valueCount, memoryResourceUri, memoryOffset,
                    memoryLength);
        } else {
            final int firstValueLength = buffer.getInt(firstValueLengthIndex);
            final int lastValueLength = buffer.getInt(lastValueLengthIndex);
            final byte[] firstValue = ByteBuffers.allocateByteArray(firstValueLength);
            int position = firstValueIndex;
            buffer.getBytes(position, firstValue);
            position += firstValueLength;
            final byte[] lastValue = ByteBuffers.allocateByteArray(lastValueLength);
            buffer.getBytes(position, lastValue);
            position += lastValueLength;
            final String memoryResourceUri = buffer.getStringUtf8(position, memoryResourceUriSize);
            return new MemoryFileSummary(firstValue, lastValue, valueCount, memoryResourceUri, memoryOffset,
                    memoryLength);
        }
    }

    @Override
    public int toBuffer(final IByteBuffer buffer, final MemoryFileSummary obj) {
        final int valueCount = obj.getValueCount();
        final String memoryResourceUri = obj.getMemoryResourceUri();
        final byte[] memoryResourceUriBytes = ByteBuffers.newStringUtf8Bytes(memoryResourceUri);
        final int memoryResourceUriSize = memoryResourceUriBytes.length;
        final long memoryOffset = obj.getMemoryOffset();
        final long memorySize = obj.getMemoryLength();
        final byte[] firstValue = obj.getFirstValue();
        final byte[] lastValue = obj.getLastValue();

        buffer.putInt(VALUECOUNT_INDEX, valueCount);
        buffer.putInt(MEMORYRESOURCEURISIZE_INDEX, memoryResourceUriSize);
        buffer.putLong(MEMORYOFFSET_INDEX, memoryOffset);
        buffer.putLong(MEMORYLENGTH_INDEX, memorySize);
        if (valueFixedLength != null) {
            buffer.putBytes(firstValueIndex, firstValue);
            buffer.putBytes(lastValueIndex, lastValue);
            buffer.putBytes(memoryResourceUriIndex, memoryResourceUriBytes);
            return memoryResourceUriIndex + memoryResourceUriBytes.length;
        } else {
            buffer.putInt(firstValueLengthIndex, firstValue.length);
            buffer.putInt(lastValueLengthIndex, lastValue.length);
            int position = firstValueIndex;
            buffer.putBytes(position, firstValue);
            position += firstValue.length;
            buffer.putBytes(position, lastValue);
            position += lastValue.length;
            buffer.putBytes(position, memoryResourceUriBytes);
            position += memoryResourceUriBytes.length;
            return position;
        }
    }

}
