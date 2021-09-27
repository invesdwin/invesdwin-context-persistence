package de.invesdwin.context.persistence.timeseriesdb.storage;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.SerdeBaseMethods;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@NotThreadSafe
public final class FileSummarySerde implements ISerde<FileSummary> {

    private static final int VALUECOUNT_INDEX = 0;
    private static final int VALUECOUNT_SIZE = Integer.BYTES;

    private static final int ADDRESSOFFSET_INDEX = VALUECOUNT_INDEX + VALUECOUNT_SIZE;
    private static final int ADDRESSOFFSET_SIZE = Long.BYTES;

    private static final int ADDRESSSIZE_INDEX = ADDRESSOFFSET_INDEX + ADDRESSOFFSET_SIZE;
    private static final int ADDRESSSIZE_SIZE = Long.BYTES;

    private static final int VALUELENGTH_SIZE = Integer.BYTES;

    private static final int FIXED_LENGTH_OVERHEAD = ADDRESSSIZE_INDEX + ADDRESSSIZE_SIZE;

    private static final int NO_FIXED_LENGTH_OVERHEAD = ADDRESSSIZE_INDEX + ADDRESSSIZE_SIZE + VALUELENGTH_SIZE
            + VALUELENGTH_SIZE;

    private final int firstValueLengthIndex;
    private final int lastValueLengthIndex;
    private final int firstValueIndex;
    private final int lastValueIndex;

    private final Integer valueFixedLength;
    private final Integer fixedLength;
    private final int allocateFixedLength;

    public FileSummarySerde(final Integer valueFixedLength) {
        this.valueFixedLength = valueFixedLength;
        if (valueFixedLength != null) {
            firstValueLengthIndex = -1;
            lastValueLengthIndex = -1;
            firstValueIndex = ADDRESSSIZE_INDEX + ADDRESSSIZE_SIZE;
            lastValueIndex = firstValueIndex + valueFixedLength;
            this.fixedLength = FIXED_LENGTH_OVERHEAD + valueFixedLength * 2;
            this.allocateFixedLength = fixedLength;
        } else {
            firstValueLengthIndex = ADDRESSSIZE_INDEX + ADDRESSSIZE_SIZE;
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
    public FileSummary fromBytes(final byte[] bytes) {
        return SerdeBaseMethods.fromBytes(this, bytes);
    }

    @Override
    public byte[] toBytes(final FileSummary obj) {
        return SerdeBaseMethods.toBytes(this, obj);
    }

    @Override
    public FileSummary fromBuffer(final IByteBuffer buffer, final int length) {
        final int valueCount = buffer.getInt(VALUECOUNT_INDEX);
        final long addressOffset = buffer.getLong(ADDRESSOFFSET_INDEX);
        final long addressSize = buffer.getLong(ADDRESSSIZE_INDEX);
        if (valueFixedLength != null) {
            final byte[] firstValue = ByteBuffers.allocateByteArray(valueFixedLength);
            buffer.getBytes(firstValueIndex, firstValue);
            final byte[] lastValue = ByteBuffers.allocateByteArray(valueFixedLength);
            buffer.getBytes(lastValueIndex, lastValue);
            return new FileSummary(firstValue, lastValue, valueCount, addressOffset, addressSize);
        } else {
            final int firstValueLength = buffer.getInt(firstValueLengthIndex);
            final int lastValueLength = buffer.getInt(lastValueLengthIndex);
            final byte[] firstValue = ByteBuffers.allocateByteArray(firstValueLength);
            buffer.getBytes(firstValueIndex, firstValue);
            final byte[] lastValue = ByteBuffers.allocateByteArray(lastValueLength);
            buffer.getBytes(firstValueIndex + firstValueLength, lastValue);
            return new FileSummary(firstValue, lastValue, valueCount, addressOffset, addressSize);
        }
    }

    @Override
    public int toBuffer(final IByteBuffer buffer, final FileSummary obj) {
        final int valueCount = obj.getValueCount();
        final long addressOffset = obj.getAddressOffset();
        final long addressSize = obj.getAddressSize();
        final byte[] firstValue = obj.getFirstValue();
        final byte[] lastValue = obj.getLastValue();

        buffer.putInt(VALUECOUNT_INDEX, valueCount);
        buffer.putLong(ADDRESSOFFSET_INDEX, addressOffset);
        buffer.putLong(ADDRESSSIZE_INDEX, addressSize);
        if (valueFixedLength != null) {
            buffer.putBytes(firstValueIndex, firstValue);
            buffer.putBytes(lastValueIndex, lastValue);
            return allocateFixedLength;
        } else {
            buffer.putInt(firstValueLengthIndex, firstValue.length);
            buffer.putInt(lastValueLengthIndex, lastValue.length);
            buffer.putBytes(firstValueIndex, firstValue);
            buffer.putBytes(firstValueIndex + firstValue.length, lastValue);
            return NO_FIXED_LENGTH_OVERHEAD + firstValue.length + lastValue.length;
        }
    }

}
