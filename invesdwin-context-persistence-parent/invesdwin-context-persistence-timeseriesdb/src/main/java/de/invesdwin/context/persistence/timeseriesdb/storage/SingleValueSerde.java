package de.invesdwin.context.persistence.timeseriesdb.storage;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@Immutable
public final class SingleValueSerde implements ISerde<SingleValue> {

    public static final SingleValueSerde GET = new SingleValueSerde();

    private SingleValueSerde() {
    }

    @Override
    public SingleValue fromBytes(final byte[] bytes) {
        if (bytes.length == 0) {
            return null;
        }
        return new SingleValue(bytes);
    }

    @Override
    public byte[] toBytes(final SingleValue obj) {
        if (obj == null) {
            return Bytes.EMPTY_ARRAY;
        }
        return obj.getBytes();
    }

    @Override
    public SingleValue fromBuffer(final IByteBuffer buffer, final int length) {
        if (length == 0) {
            return null;
        }
        final byte[] bytes = buffer.asByteArrayCopyTo(length);
        return fromBytes(bytes);
    }

    @Override
    public int toBuffer(final IByteBuffer buffer, final SingleValue obj) {
        if (obj == null) {
            return 0;
        }
        final byte[] bytes = toBytes(obj);
        buffer.putBytes(0, bytes);
        return bytes.length;
    }

}
