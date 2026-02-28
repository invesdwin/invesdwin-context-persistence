package de.invesdwin.context.persistence.timeseriesdb.array;

import java.io.Closeable;
import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.system.array.IPrimitiveArrayAllocator;
import de.invesdwin.context.system.properties.CachingDelegateProperties;
import de.invesdwin.context.system.properties.FileProperties;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.array.IBooleanArray;
import de.invesdwin.util.collections.array.IDoubleArray;
import de.invesdwin.util.collections.array.IIntegerArray;
import de.invesdwin.util.collections.array.ILongArray;
import de.invesdwin.util.collections.array.accessor.IArrayAccessor;
import de.invesdwin.util.collections.array.buffer.BufferBooleanArray;
import de.invesdwin.util.collections.array.buffer.BufferDoubleArray;
import de.invesdwin.util.collections.array.buffer.BufferIntegerArray;
import de.invesdwin.util.collections.array.buffer.BufferLongArray;
import de.invesdwin.util.collections.attributes.AttributesMap;
import de.invesdwin.util.collections.attributes.IAttributesMap;
import de.invesdwin.util.collections.bitset.IBitSet;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.Locks;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.UUIDs;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.math.BitSets;
import de.invesdwin.util.streams.buffer.bytes.FakeAllocatorBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

/**
 * Uses the flyweight pattern with memory mapped files on disk.
 */
@ThreadSafe
public class DiskPrimitiveArrayAllocator implements IPrimitiveArrayAllocator, Closeable {

    private static final AtomicInteger NEXT_ID = new AtomicInteger(Objects.hashCode(UUIDs.newPseudoRandomUUID()));
    private final DiskPrimitiveArrayAllocatorFinalizer finalizer;

    public DiskPrimitiveArrayAllocator(final String name, final File directory) {
        this.finalizer = new DiskPrimitiveArrayAllocatorFinalizer(name, directory);
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
    public IByteBuffer getByteBuffer(final String id) {
        return (IByteBuffer) finalizer.map.get(id);
    }

    @Override
    public IDoubleArray getDoubleArray(final String id) {
        return (IDoubleArray) finalizer.map.get(id);
    }

    @Override
    public IIntegerArray getIntegerArray(final String id) {
        return (IIntegerArray) finalizer.map.get(id);
    }

    @Override
    public IBooleanArray getBooleanArray(final String id) {
        return (IBooleanArray) finalizer.map.get(id);
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
        return (ILongArray) finalizer.map.get(id);
    }

    @Override
    public IByteBuffer newByteBuffer(final String id, final int size) {
        Assertions.checkNull(finalizer.map.put(id, new FakeAllocatorBuffer(nextId(), size)));
        final IByteBuffer instance = (IByteBuffer) finalizer.map.get(id);
        Assertions.checkNotNull(instance);
        return instance;
    }

    @Override
    public IDoubleArray newDoubleArray(final String id, final int size) {
        Assertions.checkNull(
                finalizer.map.put(id, new BufferDoubleArray(new FakeAllocatorBuffer(nextId(), size * Double.BYTES))));
        final IDoubleArray array = (IDoubleArray) finalizer.map.get(id);
        Assertions.checkNotNull(array);
        clearBeforeUsage(array);
        return array;
    }

    @Override
    public IIntegerArray newIntegerArray(final String id, final int size) {
        Assertions.checkNull(
                finalizer.map.put(id, new BufferIntegerArray(new FakeAllocatorBuffer(nextId(), size * Integer.BYTES))));
        final IIntegerArray array = (IIntegerArray) finalizer.map.get(id);
        Assertions.checkNotNull(array);
        clearBeforeUsage(array);
        return array;
    }

    @Override
    public IBooleanArray newBooleanArray(final String id, final int size) {
        Assertions.checkNull(finalizer.map.put(id, new BufferBooleanArray(
                new FakeAllocatorBuffer(nextId(), (BitSets.wordIndex(size - 1) + 1) * Long.BYTES), size)));
        final IBooleanArray array = (IBooleanArray) finalizer.map.get(id);
        Assertions.checkNotNull(array);
        clearBeforeUsage(array);
        return array;
    }

    @Override
    public IBitSet newBitSet(final String id, final int size) {
        final BufferBooleanArray booleanArray = (BufferBooleanArray) newBooleanArray(id, size);
        return booleanArray.getDelegate().getBitSet();
    }

    @Override
    public ILongArray newLongArray(final String id, final int size) {
        Assertions.checkNull(
                finalizer.map.put(id, new BufferLongArray(new FakeAllocatorBuffer(nextId(), size * Long.BYTES))));
        final ILongArray array = (ILongArray) finalizer.map.get(id);
        Assertions.checkNotNull(array);
        clearBeforeUsage(array);
        return array;
    }

    protected void clearBeforeUsage(final IArrayAccessor instance) {
        //make sure everything is clear since usage might only sparsely fill
        instance.clear();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(finalizer.map.getName()).addValue(getDirectory()).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(DiskPrimitiveArrayAllocator.class, finalizer.map.getName(), getDirectory());
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof DiskPrimitiveArrayAllocator) {
            final DiskPrimitiveArrayAllocator cObj = (DiskPrimitiveArrayAllocator) obj;
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
    public boolean isOnHeap(final int size) {
        return false;
    }

    private static final class DiskPrimitiveArrayAllocatorFinalizer extends AFinalizer {

        private DiskPrimitiveArrayPersistentMap<String> map;
        private AttributesMap attributes;
        private IProperties properties;

        private DiskPrimitiveArrayAllocatorFinalizer(final String name, final File directory) {
            this.map = new DiskPrimitiveArrayPersistentMap<>(name, directory);
        }

        @Override
        protected void clean() {
            final DiskPrimitiveArrayPersistentMap<String> mapCopy = map;
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
