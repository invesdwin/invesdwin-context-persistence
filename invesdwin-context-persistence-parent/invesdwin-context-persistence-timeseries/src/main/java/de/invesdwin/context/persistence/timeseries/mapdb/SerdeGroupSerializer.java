package de.invesdwin.context.persistence.timeseries.mapdb;

import java.io.DataOutput;
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
        if (out.pos != 0) {
            throw new IllegalArgumentException("Expecting out.pos to be 0");
        }
        final IByteBuffer buffer = new ArrayExpandableByteBuffer(out.buf);
        final int length = serde.toBuffer(buffer, value);
        buffer.getBytesTo(0, (DataOutput) out, length);
        out.buf = buffer.byteArray();
        out.pos = length;
        out.sizeMask = 0xFFFFFFFF - (out.buf.length - 1);
    }

    @Override
    public T deserialize(final DataInput2 input, final int available) throws IOException {
        final byte[] internalByteArray = input.internalByteArray();
        if (internalByteArray != null) {
            final int offset = input.getPos();
            final IByteBuffer buffer = ByteBuffers.wrap(internalByteArray, offset, available);
            input.setPos(offset + available);
            return serde.fromBuffer(buffer, available);
        }
        final java.nio.ByteBuffer internalByteBuffer = input.internalByteBuffer();
        if (internalByteBuffer != null) {
            final int offset = input.getPos();
            final IByteBuffer buffer = ByteBuffers.wrap(internalByteArray, offset, available);
            input.setPos(offset + available);
            return serde.fromBuffer(buffer, available);
        }
        final IByteBuffer buffer = ByteBuffers.EXPANDABLE_POOL.borrowObject();
        try {
            buffer.putBytesTo(0, input, available);
            return serde.fromBuffer(buffer, available);
        } finally {
            ByteBuffers.EXPANDABLE_POOL.returnObject(buffer);
        }
    }

    @Override
    public boolean isTrusted() {
        return true;
    }
}