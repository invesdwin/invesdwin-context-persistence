package de.invesdwin.context.persistence.timeseriesdb.array.large;

import java.io.Closeable;
import java.io.File;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.system.array.large.ILargeArrayAllocator;
import de.invesdwin.context.system.array.large.OnHeapLargeArrayAllocator;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.collections.array.large.IBooleanLargeArray;
import de.invesdwin.util.collections.array.large.IDoubleLargeArray;
import de.invesdwin.util.collections.array.large.IIntegerLargeArray;
import de.invesdwin.util.collections.array.large.ILongLargeArray;
import de.invesdwin.util.collections.array.large.bitset.ILargeBitSet;
import de.invesdwin.util.collections.attributes.IAttributesMap;
import de.invesdwin.util.collections.loadingcache.historical.AHistoricalCache;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.streams.buffer.memory.IMemoryBuffer;

@ThreadSafe
public class ThresholdDiskLargeArrayAllocator implements ILargeArrayAllocator, Closeable {

    /**
     * There might be a better optimum at which onHeap becomes slower than MemoryMappedFiles, but we currently want to
     * minimize HEAP usage, so we set a low threshold
     */
    public static final int DEFAULT_DISK_THRESHOLD = AHistoricalCache.DEFAULT_MAXIMUM_SIZE_LIMIT;
    private final long diskThreshold;
    private final ILargeArrayAllocator heap;
    private final ILargeArrayAllocator disk;

    public ThresholdDiskLargeArrayAllocator(final String name) {
        this(name, DEFAULT_DISK_THRESHOLD);
    }

    public ThresholdDiskLargeArrayAllocator(final String name, final long diskThreshold) {
        this.diskThreshold = diskThreshold;
        this.heap = newHeapLargeArrayAllocator(name);
        this.disk = newDiskLargeArrayAllocator(name);
    }

    protected ILargeArrayAllocator newHeapLargeArrayAllocator(final String name) {
        return new OnHeapLargeArrayAllocator();
    }

    protected ILargeArrayAllocator newDiskLargeArrayAllocator(final String name) {
        return new TemporaryDiskLargeArrayAllocator(name);
    }

    @Override
    public IMemoryBuffer getMemoryBuffer(final String id) {
        final IMemoryBuffer heapArr = heap.getMemoryBuffer(id);
        if (heapArr != null) {
            return heapArr;
        }
        return disk.getMemoryBuffer(id);
    }

    @Override
    public IDoubleLargeArray getDoubleArray(final String id) {
        final IDoubleLargeArray heapArr = heap.getDoubleArray(id);
        if (heapArr != null) {
            return heapArr;
        }
        return disk.getDoubleArray(id);
    }

    @Override
    public IIntegerLargeArray getIntegerArray(final String id) {
        final IIntegerLargeArray heapArr = heap.getIntegerArray(id);
        if (heapArr != null) {
            return heapArr;
        }
        return disk.getIntegerArray(id);
    }

    @Override
    public IBooleanLargeArray getBooleanArray(final String id) {
        final IBooleanLargeArray heapArr = heap.getBooleanArray(id);
        if (heapArr != null) {
            return heapArr;
        }
        return disk.getBooleanArray(id);
    }

    @Override
    public ILargeBitSet getBitSet(final String id) {
        final ILargeBitSet heapArr = heap.getBitSet(id);
        if (heapArr != null) {
            return heapArr;
        }
        return disk.getBitSet(id);
    }

    @Override
    public ILongLargeArray getLongArray(final String id) {
        final ILongLargeArray heapArr = heap.getLongArray(id);
        if (heapArr != null) {
            return heapArr;
        }
        return disk.getLongArray(id);
    }

    @Override
    public IMemoryBuffer newMemoryBuffer(final String id, final long size) {
        if (isOnHeap(size)) {
            return heap.newMemoryBuffer(id, size);
        } else {
            return disk.newMemoryBuffer(id, size);
        }
    }

    @Override
    public IDoubleLargeArray newDoubleArray(final String id, final long size) {
        if (isOnHeap(size)) {
            return heap.newDoubleArray(id, size);
        } else {
            return disk.newDoubleArray(id, size);
        }
    }

    @Override
    public IIntegerLargeArray newIntegerArray(final String id, final long size) {
        if (isOnHeap(size)) {
            return heap.newIntegerArray(id, size);
        } else {
            return disk.newIntegerArray(id, size);
        }
    }

    @Override
    public IBooleanLargeArray newBooleanArray(final String id, final long size) {
        if (isOnHeap(size)) {
            return heap.newBooleanArray(id, size);
        } else {
            return disk.newBooleanArray(id, size);
        }
    }

    @Override
    public ILargeBitSet newBitSet(final String id, final long size) {
        if (isOnHeap(size)) {
            return heap.newBitSet(id, size);
        } else {
            return disk.newBitSet(id, size);
        }
    }

    @Override
    public ILongLargeArray newLongArray(final String id, final long size) {
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
    public boolean isOnHeap(final long size) {
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
        return Objects.hashCode(ThresholdDiskLargeArrayAllocator.class, heap, disk, getDirectory());
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof ThresholdDiskLargeArrayAllocator) {
            final ThresholdDiskLargeArrayAllocator cObj = (ThresholdDiskLargeArrayAllocator) obj;
            return Objects.equals(heap, cObj.heap) && Objects.equals(disk, cObj.disk);
        } else {
            return false;
        }
    }
}
