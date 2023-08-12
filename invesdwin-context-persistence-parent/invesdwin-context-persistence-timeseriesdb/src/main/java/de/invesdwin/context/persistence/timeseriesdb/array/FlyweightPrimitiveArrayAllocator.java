package de.invesdwin.context.persistence.timeseriesdb.array;

import java.io.File;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.array.IBooleanArray;
import de.invesdwin.util.collections.array.IDoubleArray;
import de.invesdwin.util.collections.array.IIntegerArray;
import de.invesdwin.util.collections.array.ILongArray;
import de.invesdwin.util.collections.array.allocator.IPrimitiveArrayAllocator;
import de.invesdwin.util.collections.array.buffer.BufferBooleanArray;
import de.invesdwin.util.collections.array.buffer.BufferDoubleArray;
import de.invesdwin.util.collections.array.buffer.BufferIntegerArray;
import de.invesdwin.util.collections.array.buffer.BufferLongArray;
import de.invesdwin.util.collections.bitset.IBitSet;
import de.invesdwin.util.math.BitSets;
import de.invesdwin.util.streams.buffer.bytes.FakeAllocatorBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@ThreadSafe
public class FlyweightPrimitiveArrayAllocator implements IPrimitiveArrayAllocator {

    private final FlyweightPrimitiveArrayPersistentMap<String> map;

    public FlyweightPrimitiveArrayAllocator(final String name, final File directory) {
        this.map = new FlyweightPrimitiveArrayPersistentMap<>(name, directory);
    }

    @Override
    public IByteBuffer getByteBuffer(final String id) {
        return (IByteBuffer) map.get(id);
    }

    @Override
    public IDoubleArray getDoubleArray(final String id) {
        return (IDoubleArray) map.get(id);
    }

    @Override
    public IIntegerArray getIntegerArray(final String id) {
        return (IIntegerArray) map.get(id);
    }

    @Override
    public IBooleanArray getBooleanArray(final String id) {
        return (IBooleanArray) map.get(id);
    }

    @Override
    public IBitSet getBitSet(final String id) {
        final BufferBooleanArray booleanArray = (BufferBooleanArray) getBooleanArray(id);
        if (booleanArray != null) {
            return booleanArray.getDelegate().getBitSet();
        } else {
            return null;
        }
    }

    @Override
    public ILongArray getLongArray(final String id) {
        return (ILongArray) map.get(id);
    }

    @Override
    public IByteBuffer newByteBuffer(final String id, final int size) {
        Assertions.checkNull(map.put(id, new FakeAllocatorBuffer(size)));
        final IByteBuffer instance = (IByteBuffer) map.get(id);
        Assertions.checkNotNull(instance);
        return instance;
    }

    @Override
    public IDoubleArray newDoubleArray(final String id, final int size) {
        Assertions.checkNull(map.put(id, new BufferDoubleArray(new FakeAllocatorBuffer(size * Double.BYTES))));
        final IDoubleArray instance = (IDoubleArray) map.get(id);
        Assertions.checkNotNull(instance);
        return instance;
    }

    @Override
    public IIntegerArray newIntegerArray(final String id, final int size) {
        Assertions.checkNull(map.put(id, new BufferIntegerArray(new FakeAllocatorBuffer(size * Integer.BYTES))));
        final IIntegerArray instance = (IIntegerArray) map.get(id);
        Assertions.checkNotNull(instance);
        return instance;
    }

    @Override
    public IBooleanArray newBooleanArray(final String id, final int size) {
        Assertions.checkNull(map.put(id, new BufferBooleanArray(new FakeAllocatorBuffer(BitSets.wordIndex(size)))));
        final IBooleanArray instance = (IBooleanArray) map.get(id);
        Assertions.checkNotNull(instance);
        return instance;
    }

    @Override
    public IBitSet newBitSet(final String id, final int size) {
        final BufferBooleanArray booleanArray = (BufferBooleanArray) newBooleanArray(id, size);
        return booleanArray.getDelegate().getBitSet();
    }

    @Override
    public ILongArray newLongArray(final String id, final int size) {
        Assertions.checkNull(map.put(id, new BufferLongArray(new FakeAllocatorBuffer(size * Long.BYTES))));
        final ILongArray instance = (ILongArray) map.get(id);
        Assertions.checkNotNull(instance);
        return instance;
    }

}
