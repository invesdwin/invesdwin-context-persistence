package de.invesdwin.context.persistence.timeseriesdb.array.primitive;

import java.io.IOException;
import java.util.function.Function;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.collections.array.primitive.IBooleanPrimitiveArray;
import de.invesdwin.util.collections.array.primitive.IDoublePrimitiveArray;
import de.invesdwin.util.collections.array.primitive.IIntegerPrimitiveArray;
import de.invesdwin.util.collections.array.primitive.ILongPrimitiveArray;
import de.invesdwin.util.collections.array.primitive.IPrimitiveArray;
import de.invesdwin.util.collections.array.primitive.bitset.IPrimitiveBitSet;
import de.invesdwin.util.collections.array.primitive.bitset.LongArrayPrimitiveBitSet;
import de.invesdwin.util.collections.array.primitive.buffer.BufferBooleanPrimitiveArray;
import de.invesdwin.util.collections.array.primitive.buffer.BufferDoublePrimitiveArray;
import de.invesdwin.util.collections.array.primitive.buffer.BufferIntegerPrimitiveArray;
import de.invesdwin.util.collections.array.primitive.buffer.BufferLongPrimitiveArray;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.ISerdeLengthProvider;
import de.invesdwin.util.marshallers.serde.SerdeBaseMethods;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedDelegateByteBuffer;

@Immutable
public class DiskPrimitiveArraySerde implements ISerde<IPrimitiveArray>, ISerdeLengthProvider<IPrimitiveArray> {

    public static final Function<LongArrayPrimitiveBitSet, IPrimitiveBitSet> BOOLEAN_COPY_FACTORY = LongArrayPrimitiveBitSet.DEFAULT_COPY_FACTORY;

    public static final DiskPrimitiveArraySerde GET = new DiskPrimitiveArraySerde();

    private static final int ID_INDEX = 0;
    private static final int ID_LENGTH = Integer.BYTES;

    private static final int TYPE_INDEX = ID_INDEX + ID_LENGTH;
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
        final int id = buffer.getInt(ID_INDEX);
        final int typeOrdinal = buffer.getInt(TYPE_INDEX);
        final IByteBuffer arrayBuffer = new FlyweightByteBuffer(buffer, id);
        final DiskPrimitiveArrayType type = DiskPrimitiveArrayType.values()[typeOrdinal];
        switch (type) {
        case Byte:
            return arrayBuffer;
        case Boolean:
            return new BufferBooleanPrimitiveArray(BOOLEAN_COPY_FACTORY, arrayBuffer);
        case Double:
            return new BufferDoublePrimitiveArray(arrayBuffer);
        case Long:
            return new BufferLongPrimitiveArray(arrayBuffer);
        case Int:
            return new BufferIntegerPrimitiveArray(arrayBuffer);
        default:
            throw UnknownArgumentException.newInstance(DiskPrimitiveArrayType.class, type);
        }
    }

    @Override
    public int toBuffer(final IByteBuffer buffer, final IPrimitiveArray obj) {
        final int id = obj.getId();
        buffer.putInt(ID_INDEX, id);
        if (obj instanceof IByteBuffer) {
            buffer.putInt(TYPE_INDEX, (byte) DiskPrimitiveArrayType.Byte.ordinal());
        } else if (obj instanceof IBooleanPrimitiveArray) {
            buffer.putInt(TYPE_INDEX, (byte) DiskPrimitiveArrayType.Boolean.ordinal());
        } else if (obj instanceof IDoublePrimitiveArray) {
            buffer.putInt(TYPE_INDEX, (byte) DiskPrimitiveArrayType.Double.ordinal());
        } else if (obj instanceof IIntegerPrimitiveArray) {
            buffer.putInt(TYPE_INDEX, (byte) DiskPrimitiveArrayType.Int.ordinal());
        } else if (obj instanceof ILongPrimitiveArray) {
            buffer.putInt(TYPE_INDEX, (byte) DiskPrimitiveArrayType.Long.ordinal());
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

    private static final class FlyweightByteBuffer extends SlicedDelegateByteBuffer {
        private final int id;

        private FlyweightByteBuffer(final IByteBuffer delegate, final int id) {
            super(delegate, ARRAY_INDEX, delegate.remaining(ARRAY_INDEX));
            this.id = id;
        }

        @Override
        public int getId() {
            return id;
        }
    }

    @Override
    public int getLength(final IPrimitiveArray obj) {
        return ARRAY_INDEX + obj.getBufferLength();
    }

}
