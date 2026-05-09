package de.invesdwin.context.persistence.timeseriesdb.array.large;

import java.io.IOException;
import java.util.function.Function;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.timeseriesdb.array.primitive.DiskPrimitiveArrayType;
import de.invesdwin.util.collections.array.large.IBooleanLargeArray;
import de.invesdwin.util.collections.array.large.IDoubleLargeArray;
import de.invesdwin.util.collections.array.large.IIntegerLargeArray;
import de.invesdwin.util.collections.array.large.ILargeArray;
import de.invesdwin.util.collections.array.large.ILongLargeArray;
import de.invesdwin.util.collections.array.large.bitset.ILargeBitSet;
import de.invesdwin.util.collections.array.large.bitset.LongArrayLargeBitSet;
import de.invesdwin.util.collections.array.large.buffer.BufferBooleanLargeArray;
import de.invesdwin.util.collections.array.large.buffer.BufferDoubleLargeArray;
import de.invesdwin.util.collections.array.large.buffer.BufferIntegerLargeArray;
import de.invesdwin.util.collections.array.large.buffer.BufferLongLargeArray;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.marshallers.serde.large.ILargeSerde;
import de.invesdwin.util.marshallers.serde.large.ILargeSerdeLengthProvider;
import de.invesdwin.util.streams.buffer.memory.IMemoryBuffer;
import de.invesdwin.util.streams.buffer.memory.delegate.slice.SlicedDelegateMemoryBuffer;

@Immutable
public class DiskLargeArraySerde implements ILargeSerde<ILargeArray>, ILargeSerdeLengthProvider<ILargeArray> {

    public static final Function<LongArrayLargeBitSet, ILargeBitSet> BOOLEAN_COPY_FACTORY = LongArrayLargeBitSet.DEFAULT_COPY_FACTORY;

    public static final DiskLargeArraySerde GET = new DiskLargeArraySerde();

    private static final long ID_INDEX = 0;
    private static final long ID_LENGTH = Integer.BYTES;

    private static final long TYPE_INDEX = ID_INDEX + ID_LENGTH;
    //use integer for memory alignment instead of byte (which would suffice)
    private static final long TYPE_LENGTH = Integer.BYTES;

    private static final long ARRAY_INDEX = TYPE_INDEX + TYPE_LENGTH;

    @Override
    public ILargeArray fromBuffer(final IMemoryBuffer buffer) {
        final int id = buffer.getInt(ID_INDEX);
        final int typeOrdinal = buffer.getInt(TYPE_INDEX);
        final IMemoryBuffer arrayBuffer = new FlyweightMemoryBuffer(buffer, id);
        final DiskPrimitiveArrayType type = DiskPrimitiveArrayType.values()[typeOrdinal];
        switch (type) {
        case Byte:
            return arrayBuffer;
        case Boolean:
            return new BufferBooleanLargeArray(BOOLEAN_COPY_FACTORY, arrayBuffer);
        case Double:
            return new BufferDoubleLargeArray(arrayBuffer);
        case Long:
            return new BufferLongLargeArray(arrayBuffer);
        case Int:
            return new BufferIntegerLargeArray(arrayBuffer);
        default:
            throw UnknownArgumentException.newInstance(DiskPrimitiveArrayType.class, type);
        }
    }

    @Override
    public long toBuffer(final IMemoryBuffer buffer, final ILargeArray obj) {
        final int id = obj.getId();
        buffer.putInt(ID_INDEX, id);
        if (obj instanceof IMemoryBuffer) {
            buffer.putInt(TYPE_INDEX, (byte) DiskPrimitiveArrayType.Byte.ordinal());
        } else if (obj instanceof IBooleanLargeArray) {
            buffer.putInt(TYPE_INDEX, (byte) DiskPrimitiveArrayType.Boolean.ordinal());
        } else if (obj instanceof IDoubleLargeArray) {
            buffer.putInt(TYPE_INDEX, (byte) DiskPrimitiveArrayType.Double.ordinal());
        } else if (obj instanceof IIntegerLargeArray) {
            buffer.putInt(TYPE_INDEX, (byte) DiskPrimitiveArrayType.Int.ordinal());
        } else if (obj instanceof ILongLargeArray) {
            buffer.putInt(TYPE_INDEX, (byte) DiskPrimitiveArrayType.Long.ordinal());
        } else {
            throw UnknownArgumentException.newInstance(Class.class, obj.getClass());
        }
        try {
            final long length = obj.getBuffer(buffer.sliceFrom(ARRAY_INDEX));
            return ARRAY_INDEX + length;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final class FlyweightMemoryBuffer extends SlicedDelegateMemoryBuffer {
        private final int id;

        private FlyweightMemoryBuffer(final IMemoryBuffer delegate, final int id) {
            super(delegate, ARRAY_INDEX, delegate.remaining(ARRAY_INDEX));
            this.id = id;
        }

        @Override
        public int getId() {
            return id;
        }
    }

    @Override
    public long getLength(final ILargeArray obj) {
        return ARRAY_INDEX + obj.getBufferLength();
    }

}
