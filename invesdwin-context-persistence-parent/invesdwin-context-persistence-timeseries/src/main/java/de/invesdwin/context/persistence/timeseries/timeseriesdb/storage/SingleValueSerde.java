package de.invesdwin.context.persistence.timeseries.timeseriesdb.storage;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.lang.buffer.IByteBuffer;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.SerdeBaseMethods;
import de.invesdwin.util.math.Bytes;

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
        return SerdeBaseMethods.fromBuffer(this, buffer, length);
    }

    @Override
    public int toBuffer(final SingleValue obj, final IByteBuffer buffer) {
        return SerdeBaseMethods.toBuffer(this, obj, buffer);
    }

}
