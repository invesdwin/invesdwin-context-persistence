package de.invesdwin.context.persistence.timeseries.serde;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import ezdb.serde.Serde;

@Immutable
public class FixedLengthBufferingIteratorDelegateSerde<E> implements Serde<IBufferingIterator<? extends E>> {

    private final Serde<E> delegate;
    private final int fixedLength;

    @SuppressWarnings("unchecked")
    public FixedLengthBufferingIteratorDelegateSerde(final Serde<? extends E> delegate, final int fixedLength) {
        this.delegate = (Serde<E>) delegate;
        this.fixedLength = fixedLength;
    }

    @Override
    public IBufferingIterator<? extends E> fromBytes(final byte[] bytes) {
        final int size = bytes.length / fixedLength;
        final BufferingIterator<E> result = new BufferingIterator<E>();
        int curOffset = 0;
        final byte[] byteBuffer = new byte[fixedLength];
        for (int i = 0; i < size; i++) {
            System.arraycopy(bytes, curOffset, byteBuffer, 0, fixedLength);
            final E obj = delegate.fromBytes(byteBuffer);
            result.add(obj);
            curOffset += fixedLength;
        }
        return result;
    }

    @Override
    public byte[] toBytes(final IBufferingIterator<? extends E> objs) {
        final byte[] result = new byte[objs.size() * fixedLength];
        int curOffset = 0;
        for (final E obj : objs) {
            final byte[] objResult = delegate.toBytes(obj);
            if (objResult.length != fixedLength) {
                throw new IllegalArgumentException("Serialized object [" + obj + "] has unexpected byte length of ["
                        + objResult.length + "] while fixed length [" + fixedLength + "] was expected!");
            }
            System.arraycopy(objResult, 0, result, curOffset, fixedLength);
            curOffset += fixedLength;
        }
        return result;
    }

}
