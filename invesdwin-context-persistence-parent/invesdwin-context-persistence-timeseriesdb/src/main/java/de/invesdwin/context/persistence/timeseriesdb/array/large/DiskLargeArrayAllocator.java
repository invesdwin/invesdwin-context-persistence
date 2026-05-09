package de.invesdwin.context.persistence.timeseriesdb.array.large;

import java.io.Closeable;
import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.system.array.large.ILargeArrayAllocator;
import de.invesdwin.context.system.properties.CachingDelegateProperties;
import de.invesdwin.context.system.properties.FileProperties;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.array.large.IBooleanLargeArray;
import de.invesdwin.util.collections.array.large.IDoubleLargeArray;
import de.invesdwin.util.collections.array.large.IIntegerLargeArray;
import de.invesdwin.util.collections.array.large.ILongLargeArray;
import de.invesdwin.util.collections.array.large.accessor.ILargeArrayAccessor;
import de.invesdwin.util.collections.array.large.bitset.ILargeBitSet;
import de.invesdwin.util.collections.array.large.buffer.BufferBooleanLargeArray;
import de.invesdwin.util.collections.array.large.buffer.BufferDoubleLargeArray;
import de.invesdwin.util.collections.array.large.buffer.BufferIntegerLargeArray;
import de.invesdwin.util.collections.array.large.buffer.BufferLongLargeArray;
import de.invesdwin.util.collections.attributes.AttributesMap;
import de.invesdwin.util.collections.attributes.IAttributesMap;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.Locks;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.UUIDs;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.math.BitSets;
import de.invesdwin.util.streams.buffer.memory.FakeAllocatorMemoryBuffer;
import de.invesdwin.util.streams.buffer.memory.IMemoryBuffer;

/**
 * Uses the flyweight pattern with memory mapped files on disk.
 */
@ThreadSafe
public class DiskLargeArrayAllocator implements ILargeArrayAllocator, Closeable {

    private static final AtomicInteger NEXT_ID = new AtomicInteger(Objects.hashCode(UUIDs.newPseudoRandomUUID()));
    private final DiskLargeArrayAllocatorFinalizer finalizer;

    public DiskLargeArrayAllocator(final String name, final File directory) {
        this.finalizer = new DiskLargeArrayAllocatorFinalizer(name, directory);
        this.finalizer.register(this);
    }

    private int nextId() {
        return NEXT_ID.getAndIncrement();
    }

    @Override
    public File getDirectory() {
        return finalizer.map.getDirectory();
    }

    @Override
    public IMemoryBuffer getMemoryBuffer(final String id) {
        return (IMemoryBuffer) finalizer.map.get(id);
    }

    @Override
    public IDoubleLargeArray getDoubleArray(final String id) {
        return (IDoubleLargeArray) finalizer.map.get(id);
    }

    @Override
    public IIntegerLargeArray getIntegerArray(final String id) {
        return (IIntegerLargeArray) finalizer.map.get(id);
    }

    @Override
    public IBooleanLargeArray getBooleanArray(final String id) {
        return (IBooleanLargeArray) finalizer.map.get(id);
    }

    @Override
    public ILargeBitSet getBitSet(final String id) {
        final BufferBooleanLargeArray booleanArray = (BufferBooleanLargeArray) getBooleanArray(id);
        if (booleanArray != null) {
            return booleanArray.getDelegate().getBitSet();
        } else {
            return null;
        }
    }

    @Override
    public ILongLargeArray getLongArray(final String id) {
        return (ILongLargeArray) finalizer.map.get(id);
    }

    @Override
    public IMemoryBuffer newMemoryBuffer(final String id, final long size) {
        Assertions.checkNull(finalizer.map.put(id, new FakeAllocatorMemoryBuffer(nextId(), size)));
        final IMemoryBuffer array = (IMemoryBuffer) finalizer.map.get(id);
        Assertions.checkNotNull(array);
        clearBeforeUsage(array);
        return array;
    }

    @Override
    public IDoubleLargeArray newDoubleArray(final String id, final long size) {
        Assertions.checkNull(finalizer.map.put(id,
                new BufferDoubleLargeArray(new FakeAllocatorMemoryBuffer(nextId(), size * Double.BYTES))));
        final IDoubleLargeArray array = (IDoubleLargeArray) finalizer.map.get(id);
        Assertions.checkNotNull(array);
        clearBeforeUsage(array);
        return array;
    }

    @Override
    public IIntegerLargeArray newIntegerArray(final String id, final long size) {
        Assertions.checkNull(finalizer.map.put(id,
                new BufferIntegerLargeArray(new FakeAllocatorMemoryBuffer(nextId(), size * Integer.BYTES))));
        final IIntegerLargeArray array = (IIntegerLargeArray) finalizer.map.get(id);
        Assertions.checkNotNull(array);
        clearBeforeUsage(array);
        return array;
    }

    @Override
    public IBooleanLargeArray newBooleanArray(final String id, final long size) {
        Assertions.checkNull(finalizer.map.put(id, new BufferBooleanLargeArray(DiskLargeArraySerde.BOOLEAN_COPY_FACTORY,
                new FakeAllocatorMemoryBuffer(nextId(), (BitSets.wordIndex(size - 1) + 1) * Long.BYTES), size)));
        final IBooleanLargeArray array = (IBooleanLargeArray) finalizer.map.get(id);
        Assertions.checkNotNull(array);
        clearBeforeUsage(array);
        return array;
    }

    @Override
    public ILargeBitSet newBitSet(final String id, final long size) {
        final BufferBooleanLargeArray booleanArray = (BufferBooleanLargeArray) newBooleanArray(id, size);
        return booleanArray.getDelegate().getBitSet();
    }

    @Override
    public ILongLargeArray newLongArray(final String id, final long size) {
        Assertions.checkNull(finalizer.map.put(id,
                new BufferLongLargeArray(new FakeAllocatorMemoryBuffer(nextId(), size * Long.BYTES))));
        final ILongLargeArray array = (ILongLargeArray) finalizer.map.get(id);
        Assertions.checkNotNull(array);
        clearBeforeUsage(array);
        return array;
    }

    protected void clearBeforeUsage(final ILargeArrayAccessor instance) {
        //make sure everything is clear since usage might only sparsely fill
        instance.clear();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(finalizer.map.getName()).addValue(getDirectory()).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(DiskLargeArrayAllocator.class, finalizer.map.getName(), getDirectory());
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof DiskLargeArrayAllocator) {
            final DiskLargeArrayAllocator cObj = (DiskLargeArrayAllocator) obj;
            return Objects.equals(finalizer.map.getName(), cObj.finalizer.map.getDirectory())
                    && Objects.equals(getDirectory(), cObj.getDirectory());
        } else {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(final Class<T> type) {
        if (type.isAssignableFrom(getClass())) {
            return (T) this;
        } else {
            return null;
        }
    }

    @Override
    public IAttributesMap getAttributes() {
        if (finalizer.attributes == null) {
            synchronized (this) {
                if (finalizer.attributes == null) {
                    finalizer.attributes = new AttributesMap();
                }
            }
        }
        return finalizer.attributes;
    }

    @Override
    public IProperties getProperties() {
        if (finalizer.properties == null) {
            synchronized (this) {
                if (finalizer.properties == null) {
                    finalizer.properties = new CachingDelegateProperties(new FileProperties(newPropertiesFile()));
                }
            }
        }
        return finalizer.properties;
    }

    private File newPropertiesFile() {
        return new File(getDirectory(), finalizer.map.getName() + ".properties");
    }

    @Override
    public void clear() {
        finalizer.map.deleteTable();
        final AttributesMap attributesCopy = finalizer.attributes;
        if (attributesCopy != null) {
            attributesCopy.clear();
            finalizer.attributes = null;
        }
        finalizer.properties = null;
    }

    @Override
    public void close() {
        finalizer.close();
    }

    /**
     * Could use FileChannelLock with thread lock enabled here if we want to support multiple processes accessing the
     * same storage. Though there are a few more things that need to be changed for this to work.
     */
    @Override
    public ILock getLock(final String id) {
        return (ILock) getAttributes().computeIfAbsent(id, (k) -> Locks.newReentrantLock(k));
    }

    @Override
    public boolean isOnHeap(final long size) {
        return false;
    }

    private static final class DiskLargeArrayAllocatorFinalizer extends AFinalizer {

        private DiskLargeArrayPersistentMap<String> map;
        private AttributesMap attributes;
        private IProperties properties;

        private DiskLargeArrayAllocatorFinalizer(final String name, final File directory) {
            this.map = new DiskLargeArrayPersistentMap<>(name, directory);
        }

        @Override
        protected void clean() {
            final DiskLargeArrayPersistentMap<String> mapCopy = map;
            if (mapCopy != null) {
                mapCopy.close();
                map = null;
            }
            attributes = null;
            properties = null;
        }

        @Override
        protected boolean isCleaned() {
            return map == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }
    }

}
