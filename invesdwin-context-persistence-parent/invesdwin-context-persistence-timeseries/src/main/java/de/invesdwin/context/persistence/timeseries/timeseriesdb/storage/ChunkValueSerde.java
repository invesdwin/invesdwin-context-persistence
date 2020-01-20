package de.invesdwin.context.persistence.timeseries.timeseriesdb.storage;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.math.Bytes;
import ezdb.serde.Serde;

@NotThreadSafe
public final class ChunkValueSerde implements Serde<ChunkValue> {

    private static final int NO_FIXED_LENGTH_OVERHEAD = Integer.BYTES + Integer.BYTES + Integer.BYTES;
    private final Integer valueFixedLength;
    private final Integer fixedLength;

    public ChunkValueSerde(final Integer valueFixedLength) {
        this.valueFixedLength = valueFixedLength;
        if (valueFixedLength != null) {
            this.fixedLength = valueFixedLength * 2 + Integer.BYTES;
        } else {
            this.fixedLength = null;
        }
    }

    public Integer getFixedLength() {
        return fixedLength;
    }

    @Override
    public ChunkValue fromBytes(final byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        final int count = buffer.getInt();
        if (valueFixedLength != null) {
            final byte[] firstValue = new byte[valueFixedLength];
            buffer.get(firstValue);
            final byte[] lastValue = new byte[valueFixedLength];
            buffer.get(lastValue);
            return new ChunkValue(firstValue, lastValue, count);
        } else {
            final int firstValueLength = buffer.getInt();
            final int lastValueLength = buffer.getInt();
            final byte[] firstValue = new byte[firstValueLength];
            buffer.get(firstValue);
            final byte[] lastValue = new byte[lastValueLength];
            buffer.get(lastValue);
            return new ChunkValue(firstValue, lastValue, count);
        }
    }

    @Override
    public byte[] toBytes(final ChunkValue obj) {
        if (obj == null) {
            return Bytes.EMPTY_ARRAY;
        }

        final int count = obj.getCount();
        final byte[] firstValue = obj.getFirstValue();
        final byte[] lastValue = obj.getLastValue();

        if (valueFixedLength == null) {
            final ByteBuffer buffer = ByteBuffer.allocate(NO_FIXED_LENGTH_OVERHEAD + firstValue.length + lastValue.length);
            buffer.putInt(count);
            buffer.putInt(firstValue.length);
            buffer.putInt(lastValue.length);
            buffer.put(firstValue);
            buffer.put(lastValue);
            return buffer.array();
        } else {
            final ByteBuffer buffer = ByteBuffer.allocate(fixedLength);
            buffer.putInt(count);
            buffer.put(firstValue);
            buffer.put(lastValue);
            return buffer.array();
        }
    }

}
