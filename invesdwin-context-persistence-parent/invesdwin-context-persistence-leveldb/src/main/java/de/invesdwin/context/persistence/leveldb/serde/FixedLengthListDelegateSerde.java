package de.invesdwin.context.persistence.leveldb.serde;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import ezdb.serde.Serde;

@Immutable
public class FixedLengthListDelegateSerde<E> implements Serde<List<? extends E>> {

    private final Serde<E> delegate;
    private final int fixedLength;

    public FixedLengthListDelegateSerde(final Serde<E> delegate, final int fixedLength) {
        this.delegate = delegate;
        this.fixedLength = fixedLength;
    }

    @Override
    public List<? extends E> fromBytes(final byte[] bytes) {
        final int size = bytes.length / fixedLength;
        final List<E> result = new ArrayList<E>(size);
        int curOffset = 0;
        for (int i = 0; i < size; i++) {
            final byte[] byteBuffer = new byte[fixedLength];
            System.arraycopy(bytes, curOffset, byteBuffer, 0, fixedLength);
            final E obj = delegate.fromBytes(byteBuffer);
            result.add(obj);
            curOffset += fixedLength;
        }
        return result;
    }

    @Override
    public byte[] toBytes(final List<? extends E> objs) {
        final byte[] result = new byte[objs.size() * fixedLength];
        final int size = objs.size();
        int curOffset = 0;
        for (int i = 0; i < size; i++) {
            final E obj = objs.get(i);
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
