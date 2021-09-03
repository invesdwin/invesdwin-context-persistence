package de.invesdwin.context.persistence.timeseries.mapdb;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.serializer.GroupSerializerObjectArray;

import de.invesdwin.context.integration.streams.compressor.ICompressionFactory;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.extend.ArrayExpandableByteBuffer;

@Immutable
public final class SerdeGroupSerializer<T> extends GroupSerializerObjectArray<T> {

    private final ISerde<T> serde;

    public SerdeGroupSerializer(final ISerde<T> serde, final ICompressionFactory compressionFactory) {
        this.serde = compressionFactory.maybeWrap(serde);
    }

    @Override
    public void serialize(final DataOutput2 out, final T value) throws IOException {
        final IByteBuffer buffer = new ArrayExpandableByteBuffer(out.buf);
        final int positionBefore = out.pos;
        final IByteBuffer valueBuffer = buffer.sliceFrom(positionBefore);
        final int valueLength = serde.toBuffer(valueBuffer, value);
        out.buf = buffer.byteArray();
        out.pos = positionBefore + valueLength;
        out.sizeMask = 0xFFFFFFFF - (out.buf.length - 1);
    }

    @Override
    public T deserialize(final DataInput2 input, final int available) throws IOException {
        final byte[] internalByteArray = input.internalByteArray();
        if (internalByteArray != null) {
            final int positionBefore = input.getPos();
            final IByteBuffer buffer = ByteBuffers.wrapFrom(internalByteArray, positionBefore);
            input.setPos(positionBefore + buffer.capacity());
            return serde.fromBuffer(buffer, buffer.capacity());
        }
        final java.nio.ByteBuffer internalByteBuffer = input.internalByteBuffer();
        if (internalByteBuffer != null) {
            final int positionBefore = input.getPos();
            final IByteBuffer buffer = ByteBuffers.wrapFrom(internalByteArray, positionBefore);
            input.setPos(positionBefore + buffer.capacity());
            return serde.fromBuffer(buffer, buffer.capacity());
        }
        final IByteBuffer buffer = ByteBuffers.EXPANDABLE_POOL.borrowObject();
        try {
            final int valueLength = ByteBuffers.readExpandable(input, buffer, 0);
            buffer.putBytesTo(0, input, valueLength);
            return serde.fromBuffer(buffer, valueLength);
        } finally {
            ByteBuffers.EXPANDABLE_POOL.returnObject(buffer);
        }
    }

    @Override
    public int hashCode(final T o, final int seed) {
        return super.hashCode(o, seed);
    }

    @Override
    public boolean isTrusted() {
        return true;
    }
}