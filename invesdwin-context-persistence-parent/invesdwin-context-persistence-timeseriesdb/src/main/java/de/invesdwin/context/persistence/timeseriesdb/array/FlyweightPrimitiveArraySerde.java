package de.invesdwin.context.persistence.timeseriesdb.array;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.collections.array.IBooleanArray;
import de.invesdwin.util.collections.array.IDoubleArray;
import de.invesdwin.util.collections.array.IIntegerArray;
import de.invesdwin.util.collections.array.ILongArray;
import de.invesdwin.util.collections.array.IPrimitiveArray;
import de.invesdwin.util.collections.array.buffer.BufferBooleanArray;
import de.invesdwin.util.collections.array.buffer.BufferDoubleArray;
import de.invesdwin.util.collections.array.buffer.BufferIntegerArray;
import de.invesdwin.util.collections.array.buffer.BufferLongArray;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.SerdeBaseMethods;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@Immutable
public class FlyweightPrimitiveArraySerde implements ISerde<IPrimitiveArray> {

    public static final FlyweightPrimitiveArraySerde GET = new FlyweightPrimitiveArraySerde();

    private static final int TYPE_INDEX = 0;
    //use integer for memory alignment instead of byte (which would suffice)
    private static final int TYPE_LENGTH = Integer.BYTES;

    private static final int ARRAY_INDEX = TYPE_INDEX + TYPE_LENGTH;

    @Override
    public IPrimitiveArray fromBytes(final byte[] bytes) {
        return SerdeBaseMethods.fromBytes(this, bytes);
    }

    @Override
    public byte[] toBytes(final IPrimitiveArray obj) {
        return SerdeBaseMethods.toBytes(this, obj);
    }

    @Override
    public IPrimitiveArray fromBuffer(final IByteBuffer buffer) {
        final int typeOrdinal = buffer.getInt(TYPE_INDEX);
        final IByteBuffer arrayBuffer = buffer.sliceFrom(ARRAY_INDEX);
        final FlyweightPrimitiveArrayType type = FlyweightPrimitiveArrayType.values()[typeOrdinal];
        switch (type) {
        case Byte:
            return arrayBuffer;
        case Boolean:
            return new BufferBooleanArray(arrayBuffer);
        case Double:
            return new BufferDoubleArray(arrayBuffer);
        case Long:
            return new BufferLongArray(arrayBuffer);
        case Int:
            return new BufferIntegerArray(arrayBuffer);
        default:
            throw UnknownArgumentException.newInstance(FlyweightPrimitiveArrayType.class, type);
        }
    }

    @Override
    public int toBuffer(final IByteBuffer buffer, final IPrimitiveArray obj) {
        if (obj instanceof IByteBuffer) {
            buffer.putInt(TYPE_INDEX, (byte) FlyweightPrimitiveArrayType.Byte.ordinal());
        } else if (obj instanceof IBooleanArray) {
            buffer.putInt(TYPE_INDEX, (byte) FlyweightPrimitiveArrayType.Boolean.ordinal());
        } else if (obj instanceof IDoubleArray) {
            buffer.putInt(TYPE_INDEX, (byte) FlyweightPrimitiveArrayType.Double.ordinal());
        } else if (obj instanceof IIntegerArray) {
            buffer.putInt(TYPE_INDEX, (byte) FlyweightPrimitiveArrayType.Int.ordinal());
        } else if (obj instanceof ILongArray) {
            buffer.putInt(TYPE_INDEX, (byte) FlyweightPrimitiveArrayType.Long.ordinal());
        } else {
            throw UnknownArgumentException.newInstance(Class.class, obj.getClass());
        }
        try {
            final int length = obj.getBuffer(buffer.sliceFrom(ARRAY_INDEX));
            return ARRAY_INDEX + length;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

}
