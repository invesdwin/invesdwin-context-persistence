package de.invesdwin.context.persistence.timeseries.timeseriesdb.storage;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.SerdeBaseMethods;
import de.invesdwin.util.streams.buffer.IByteBuffer;

@NotThreadSafe
public final class ChunkValueSerde implements ISerde<ChunkValue> {

    private static final int COUNT_INDEX = 0;
    private static final int COUNT_SIZE = Integer.BYTES;

    private static final int VALUELENGTH_SIZE = Integer.BYTES;

    private static final int NO_FIXED_LENGTH_OVERHEAD = Integer.BYTES + Integer.BYTES + Integer.BYTES;

    private final int firstValueLengthIndex;
    private final int lastValueLengthIndex;
    private final int firstValueIndex;
    private final int lastValueIndex;

    private final Integer valueFixedLength;
    private final Integer fixedLength;
    private final int allocateFixedLength;

    public ChunkValueSerde(final Integer valueFixedLength) {
        this.valueFixedLength = valueFixedLength;
        if (valueFixedLength != null) {
            firstValueLengthIndex = -1;
            lastValueLengthIndex = -1;
            firstValueIndex = COUNT_INDEX + COUNT_SIZE;
            lastValueIndex = firstValueIndex + valueFixedLength;
            this.fixedLength = valueFixedLength * 2 + Integer.BYTES;
            this.allocateFixedLength = fixedLength;
        } else {
            firstValueLengthIndex = COUNT_INDEX + COUNT_SIZE;
            lastValueLengthIndex = firstValueLengthIndex + VALUELENGTH_SIZE;
            firstValueIndex = lastValueLengthIndex + VALUELENGTH_SIZE;
            lastValueIndex = -1;
            this.fixedLength = null;
            this.allocateFixedLength = -1;
        }
    }

    public Integer getFixedLength() {
        return fixedLength;
    }

    @Override
    public ChunkValue fromBytes(final byte[] bytes) {
        return SerdeBaseMethods.fromBytes(this, bytes);
    }

    @Override
    public byte[] toBytes(final ChunkValue obj) {
        return SerdeBaseMethods.toBytes(this, obj, allocateFixedLength);
    }

    @Override
    public ChunkValue fromBuffer(final IByteBuffer buffer, final int length) {
        final int count = buffer.getInt(COUNT_INDEX);
        if (valueFixedLength != null) {
            final byte[] firstValue = new byte[valueFixedLength];
            buffer.getBytes(firstValueIndex, firstValue);
            final byte[] lastValue = new byte[valueFixedLength];
            buffer.getBytes(lastValueIndex, lastValue);
            return new ChunkValue(firstValue, lastValue, count);
        } else {
            final int firstValueLength = buffer.getInt(firstValueLengthIndex);
            final int lastValueLength = buffer.getInt(lastValueLengthIndex);
            final byte[] firstValue = new byte[firstValueLength];
            buffer.getBytes(firstValueIndex, firstValue);
            final byte[] lastValue = new byte[lastValueLength];
            buffer.getBytes(firstValueIndex + firstValueLength, lastValue);
            return new ChunkValue(firstValue, lastValue, count);
        }
    }

    @Override
    public int toBuffer(final IByteBuffer buffer, final ChunkValue obj) {
        final int count = obj.getCount();
        final byte[] firstValue = obj.getFirstValue();
        final byte[] lastValue = obj.getLastValue();

        if (valueFixedLength != null) {
            buffer.putInt(COUNT_INDEX, count);
            buffer.putBytes(firstValueIndex, firstValue);
            buffer.putBytes(lastValueIndex, lastValue);
            return allocateFixedLength;
        } else {
            buffer.putInt(COUNT_INDEX, count);
            buffer.putInt(firstValueLengthIndex, firstValue.length);
            buffer.putInt(lastValueLengthIndex, lastValue.length);
            buffer.putBytes(firstValueIndex, firstValue);
            buffer.putBytes(firstValueIndex + firstValue.length, lastValue);
            return NO_FIXED_LENGTH_OVERHEAD + firstValue.length + lastValue.length;
        }
    }

}
