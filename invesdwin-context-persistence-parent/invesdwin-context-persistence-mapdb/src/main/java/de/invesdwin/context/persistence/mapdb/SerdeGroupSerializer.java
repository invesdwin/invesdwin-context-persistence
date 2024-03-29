package de.invesdwin.context.persistence.mapdb;

import java.io.IOException;
import java.util.Comparator;

import javax.annotation.concurrent.Immutable;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.serializer.GroupSerializerObjectArray;

import de.invesdwin.util.lang.comparator.Comparables;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.extend.ArrayExpandableByteBuffer;

@Immutable
public final class SerdeGroupSerializer<T> extends GroupSerializerObjectArray<T> {

    private static final int SIZE_INDEX = 0;
    private static final int SIZE_SIZE = Integer.BYTES;

    private static final int VALUE_INDEX = SIZE_INDEX + SIZE_SIZE;

    private final ISerde<T> serde;
    private final Comparator<T> comparator;

    public SerdeGroupSerializer(final ISerde<T> serde) {
        this.serde = serde;
        this.comparator = Comparables.getComparatorNotNullSafe();
    }

    public SerdeGroupSerializer(final ISerde<T> serde, final Comparator<T> comparator) {
        this.serde = serde;
        this.comparator = comparator;
    }

    @Override
    public void serialize(final DataOutput2 out, final T value) throws IOException {
        final IByteBuffer buffer = new ArrayExpandableByteBuffer(out.buf);
        final int positionBefore = out.pos;
        final IByteBuffer valueBuffer = buffer.sliceFrom(positionBefore + VALUE_INDEX);
        final int valueLength = serde.toBuffer(valueBuffer, value);
        buffer.putInt(positionBefore + SIZE_INDEX, valueLength);
        out.buf = buffer.byteArray();
        out.pos = positionBefore + VALUE_INDEX + valueLength;
        out.sizeMask = 0xFFFFFFFF - (out.buf.length - 1);
    }

    @Override
    public T deserialize(final DataInput2 input, final int available) throws IOException {
        final int valueLength = input.readInt();
        final byte[] internalByteArray = input.internalByteArray();
        if (internalByteArray != null) {
            final int positionBefore = input.getPos();
            final IByteBuffer buffer = ByteBuffers.wrap(internalByteArray, positionBefore, valueLength);
            input.setPos(positionBefore + valueLength);
            return serde.fromBuffer(buffer);
        }
        final java.nio.ByteBuffer internalByteBuffer = input.internalByteBuffer();
        if (internalByteBuffer != null) {
            final int positionBefore = input.getPos();
            final IByteBuffer buffer = ByteBuffers.wrap(internalByteBuffer, positionBefore, valueLength);
            input.setPos(positionBefore + valueLength);
            return serde.fromBuffer(buffer);
        }
        try (ICloseableByteBuffer buffer = ByteBuffers.EXPANDABLE_POOL.borrowObject()) {
            buffer.putBytesTo(0, input, valueLength);
            return serde.fromBuffer(buffer.sliceTo(valueLength));
        }
    }

    @Override
    public int hashCode(final T o, final int seed) {
        return super.hashCode(o, seed);
    }

    @Override
    public int compare(final T first, final T second) {
        return comparator.compare(first, second);
    }

    @Override
    public boolean isTrusted() {
        return true;
    }
}