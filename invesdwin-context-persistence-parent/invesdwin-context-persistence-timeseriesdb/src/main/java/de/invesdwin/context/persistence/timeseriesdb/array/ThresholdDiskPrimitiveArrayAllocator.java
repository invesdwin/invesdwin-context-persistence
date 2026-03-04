package de.invesdwin.context.persistence.timeseriesdb.array;

import java.io.Closeable;
import java.io.File;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.system.array.IPrimitiveArrayAllocator;
import de.invesdwin.context.system.array.OnHeapPrimitiveArrayAllocator;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.collections.array.IBooleanArray;
import de.invesdwin.util.collections.array.IDoubleArray;
import de.invesdwin.util.collections.array.IIntegerArray;
import de.invesdwin.util.collections.array.ILongArray;
import de.invesdwin.util.collections.attributes.IAttributesMap;
import de.invesdwin.util.collections.bitset.IBitSet;
import de.invesdwin.util.collections.loadingcache.historical.AHistoricalCache;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@ThreadSafe
public class ThresholdDiskPrimitiveArrayAllocator implements IPrimitiveArrayAllocator, Closeable {

    /**
     * There might be a better optimum at which onHeap becomes slower than MemoryMappedFiles, but we currently want to
     * minimize HEAP usage, so we set a low threshold
     */
    public static final int DEFAULT_DISK_THRESHOLD = AHistoricalCache.DEFAULT_MAXIMUM_SIZE_LIMIT;
    private final int diskThreshold;
    private final IPrimitiveArrayAllocator heap;
    private final IPrimitiveArrayAllocator disk;

    public ThresholdDiskPrimitiveArrayAllocator(final String name) {
        this(name, DEFAULT_DISK_THRESHOLD);
    }

    public ThresholdDiskPrimitiveArrayAllocator(final String name, final int diskThreshold) {
        this.diskThreshold = diskThreshold;
        this.heap = newHeapPrimitiveArrayAllocator(name);
        this.disk = newDiskPrimitiveArrayAllocator(name);
    }

    protected IPrimitiveArrayAllocator newHeapPrimitiveArrayAllocator(final String name) {
        return new OnHeapPrimitiveArrayAllocator();
    }

    protected IPrimitiveArrayAllocator newDiskPrimitiveArrayAllocator(final String name) {
        return new TemporaryDiskPrimitiveArrayAllocator(name);
    }

    @Override
    public IByteBuffer getByteBuffer(final String id) {
        final IByteBuffer heapArr = heap.getByteBuffer(id);
        if (heapArr != null) {
            return heapArr;
        }
        return disk.getByteBuffer(id);
    }

    @Override
    public IDoubleArray getDoubleArray(final String id) {
        final IDoubleArray heapArr = heap.getDoubleArray(id);
        if (heapArr != null) {
            return heapArr;
        }
        return disk.getDoubleArray(id);
    }

    @Override
    public IIntegerArray getIntegerArray(final String id) {
        final IIntegerArray heapArr = heap.getIntegerArray(id);
        if (heapArr != null) {
            return heapArr;
        }
        return disk.getIntegerArray(id);
    }

    @Override
    public IBooleanArray getBooleanArray(final String id) {
        final IBooleanArray heapArr = heap.getBooleanArray(id);
        if (heapArr != null) {
            return heapArr;
        }
        return disk.getBooleanArray(id);
    }

    @Override
    public IBitSet getBitSet(final String id) {
        final IBitSet heapArr = heap.getBitSet(id);
        if (heapArr != null) {
            return heapArr;
        }
        return disk.getBitSet(id);
    }

    @Override
    public ILongArray getLongArray(final String id) {
        final ILongArray heapArr = heap.getLongArray(id);
        if (heapArr != null) {
            return heapArr;
        }
        return disk.getLongArray(id);
    }

    @Override
    public IByteBuffer newByteBuffer(final String id, final int size) {
        if (isOnHeap(size)) {
            return heap.newByteBuffer(id, size);
        } else {
            return disk.newByteBuffer(id, size);
        }
    }

    @Override
    public IDoubleArray newDoubleArray(final String id, final int size) {
        if (isOnHeap(size)) {
            return heap.newDoubleArray(id, size);
        } else {
            return disk.newDoubleArray(id, size);
        }
    }

    @Override
    public IIntegerArray newIntegerArray(final String id, final int size) {
        if (isOnHeap(size)) {
            return heap.newIntegerArray(id, size);
        } else {
            return disk.newIntegerArray(id, size);
        }
    }

    @Override
    public IBooleanArray newBooleanArray(final String id, final int size) {
        if (isOnHeap(size)) {
            return heap.newBooleanArray(id, size);
        } else {
            return disk.newBooleanArray(id, size);
        }
    }

    @Override
    public IBitSet newBitSet(final String id, final int size) {
        if (isOnHeap(size)) {
            return heap.newBitSet(id, size);
        } else {
            return disk.newBitSet(id, size);
        }
    }

    @Override
    public ILongArray newLongArray(final String id, final int size) {
        if (isOnHeap(size)) {
            return heap.newLongArray(id, size);
        } else {
            return disk.newLongArray(id, size);
        }
    }

    @Override
    public IAttributesMap getAttributes() {
        return disk.getAttributes();
    }

    @Override
    public IProperties getProperties() {
        return disk.getProperties();
    }

    @Override
    public void clear() {
        heap.clear();
        disk.clear();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(final Class<T> type) {
        if (type.isAssignableFrom(getClass())) {
            return (T) this;
        } else {
            final T heapUnwrapped = heap.unwrap(type);
            if (heapUnwrapped != null) {
                return heapUnwrapped;
            }
            return disk.unwrap(type);
        }
    }

    @Override
    public boolean isOnHeap(final int size) {
        final boolean onHeap = size < diskThreshold;
        return onHeap;
    }

    @Override
    public File getDirectory() {
        return disk.getDirectory();
    }

    @Override
    public ILock getLock(final String id) {
        return disk.getLock(id);
    }

    @Override
    public void close() {
        heap.close();
        disk.close();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(heap).addValue(disk).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(ThresholdDiskPrimitiveArrayAllocator.class, heap, disk, getDirectory());
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof ThresholdDiskPrimitiveArrayAllocator) {
            final ThresholdDiskPrimitiveArrayAllocator cObj = (ThresholdDiskPrimitiveArrayAllocator) obj;
            return Objects.equals(heap, cObj.heap) && Objects.equals(disk, cObj.disk);
        } else {
            return false;
        }
    }
}
