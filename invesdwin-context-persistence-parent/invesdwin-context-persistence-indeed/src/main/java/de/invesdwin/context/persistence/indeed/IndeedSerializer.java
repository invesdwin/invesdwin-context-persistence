package de.invesdwin.context.persistence.indeed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import com.indeed.util.serialization.Serializer;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@Immutable
public final class IndeedSerializer<E> implements Serializer<E> {

    private final ISerde<E> serde;

    private IndeedSerializer(final ISerde<E> serde) {
        this.serde = serde;
    }

    @Override
    public void write(final E t, final DataOutput out) throws IOException {
        final IByteBuffer buffer = ByteBuffers.EXPANDABLE_POOL.borrowObject();
        try {
            final int length = serde.toBuffer(buffer, t);
            out.writeInt(length);
            buffer.getBytesTo(0, out, length);
        } finally {
            ByteBuffers.EXPANDABLE_POOL.returnObject(buffer);
        }
    }

    @Override
    public E read(final DataInput in) throws IOException {
        final int length = in.readInt();
        final IByteBuffer buffer = ByteBuffers.EXPANDABLE_POOL.borrowObject();
        try {
            buffer.putBytesTo(0, in, length);
            return serde.fromBuffer(buffer, length);
        } finally {
            ByteBuffers.EXPANDABLE_POOL.returnObject(buffer);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> Serializer<T> valueOf(final ISerde<T> delegate) {
        if (delegate == null) {
            return null;
        } else if (delegate instanceof Serializer) {
            return (Serializer<T>) delegate;
        } else {
            return new IndeedSerializer<T>(delegate);
        }
    }
}