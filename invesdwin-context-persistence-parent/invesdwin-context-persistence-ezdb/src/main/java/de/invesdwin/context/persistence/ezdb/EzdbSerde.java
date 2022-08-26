package de.invesdwin.context.persistence.ezdb;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.delegate.NettyDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.delegate.NioDelegateByteBuffer;
import io.netty.buffer.ByteBuf;

@Immutable
public class EzdbSerde<O> implements ezdb.serde.Serde<O> {

    private final ISerde<O> delegate;

    public EzdbSerde(final ISerde<O> delegate) {
        this.delegate = delegate;
    }

    @Override
    public O fromBytes(final byte[] bytes) {
        return delegate.fromBytes(bytes);
    }

    @Override
    public O fromBuffer(final ByteBuf buffer) {
        final int position = buffer.readerIndex();
        final int length = buffer.readableBytes();
        final O obj = delegate.fromBuffer(new NettyDelegateByteBuffer(buffer).newSlice(position, length));
        buffer.readerIndex(length);
        return obj;
    }

    @Override
    public O fromBuffer(final java.nio.ByteBuffer buffer) {
        final int position = buffer.position();
        final int length = buffer.remaining();
        final O obj = delegate.fromBuffer(new NioDelegateByteBuffer(buffer).newSlice(position, length));
        return obj;
    }

    @Override
    public byte[] toBytes(final O obj) {
        return delegate.toBytes(obj);
    }

    @Override
    public void toBuffer(final ByteBuf buffer, final O obj) {
        final int position = buffer.writerIndex();
        final int length = delegate.toBuffer(new NettyDelegateByteBuffer(buffer).newSliceFrom(position), obj);
        buffer.writerIndex(position + length);
    }

    @Override
    public void toBuffer(final java.nio.ByteBuffer buffer, final O obj) {
        final int position = buffer.position();
        final int length = delegate.toBuffer(new NioDelegateByteBuffer(buffer).newSliceFrom(position), obj);
        buffer.limit(position + length);
    }

    @SuppressWarnings("unchecked")
    public static <T> ezdb.serde.Serde<T> valueOf(final ISerde<T> delegate) {
        if (delegate == null) {
            return null;
        } else if (delegate instanceof ezdb.serde.Serde) {
            return (ezdb.serde.Serde<T>) delegate;
        } else {
            return new EzdbSerde<T>(delegate);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(final Class<T> type) {
        if (type.isAssignableFrom(getClass())) {
            return (T) this;
        } else if (type.isAssignableFrom(delegate.getClass())) {
            return (T) delegate;
        } else {
            return null;
        }
    }

}
