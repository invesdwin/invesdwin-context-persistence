package de.invesdwin.context.persistence.timeseriesdb.array;

import java.io.Closeable;
import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.system.array.primitive.IPrimitiveArrayAllocator;
import de.invesdwin.context.system.properties.CachingDelegateProperties;
import de.invesdwin.context.system.properties.FileProperties;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.array.primitive.IBooleanPrimtiveArray;
import de.invesdwin.util.collections.array.primitive.IDoublePrimitiveArray;
import de.invesdwin.util.collections.array.primitive.IIntegerPrimitiveArray;
import de.invesdwin.util.collections.array.primitive.ILongPrimitiveArray;
import de.invesdwin.util.collections.array.primitive.accessor.IPrimitiveArrayAccessor;
import de.invesdwin.util.collections.array.primitive.bitset.IPrimitiveBitSet;
import de.invesdwin.util.collections.array.primitive.buffer.BufferBooleanPrimitiveArray;
import de.invesdwin.util.collections.array.primitive.buffer.BufferDoublePrimitiveArray;
import de.invesdwin.util.collections.array.primitive.buffer.BufferIntegerPrimitiveArray;
import de.invesdwin.util.collections.array.primitive.buffer.BufferLongPrimitiveArray;
import de.invesdwin.util.collections.attributes.AttributesMap;
import de.invesdwin.util.collections.attributes.IAttributesMap;
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
    public IDoublePrimitiveArray getDoubleArray(final String id) {
        return (IDoublePrimitiveArray) finalizer.map.get(id);
    }

    @Override
    public IIntegerPrimitiveArray getIntegerArray(final String id) {
        return (IIntegerPrimitiveArray) finalizer.map.get(id);
    }

    @Override
    public IBooleanPrimtiveArray getBooleanArray(final String id) {
        return (IBooleanPrimtiveArray) finalizer.map.get(id);
    }

    @Override
    public IPrimitiveBitSet getBitSet(final String id) {
        final BufferBooleanPrimitiveArray booleanArray = (BufferBooleanPrimitiveArray) getBooleanArray(id);
        if (booleanArray != null) {
            return booleanArray.getDelegate().getBitSet();
        } else {
            return null;
        }
    }

    @Override
    public ILongPrimitiveArray getLongArray(final String id) {
        return (ILongPrimitiveArray) finalizer.map.get(id);
    }

    @Override
    public IByteBuffer newByteBuffer(final String id, final int size) {
        Assertions.checkNull(finalizer.map.put(id, new FakeAllocatorBuffer(nextId(), size)));
        final IByteBuffer instance = (IByteBuffer) finalizer.map.get(id);
        Assertions.checkNotNull(instance);
        return instance;
    }

    @Override
    public IDoublePrimitiveArray newDoubleArray(final String id, final int size) {
        Assertions.checkNull(
                finalizer.map.put(id, new BufferDoublePrimitiveArray(new FakeAllocatorBuffer(nextId(), size * Double.BYTES))));
        final IDoublePrimitiveArray array = (IDoublePrimitiveArray) finalizer.map.get(id);
        Assertions.checkNotNull(array);
        clearBeforeUsage(array);
        return array;
    }

    @Override
    public IIntegerPrimitiveArray newIntegerArray(final String id, final int size) {
        Assertions.checkNull(
                finalizer.map.put(id, new BufferIntegerPrimitiveArray(new FakeAllocatorBuffer(nextId(), size * Integer.BYTES))));
        final IIntegerPrimitiveArray array = (IIntegerPrimitiveArray) finalizer.map.get(id);
        Assertions.checkNotNull(array);
        clearBeforeUsage(array);
        return array;
    }

    @Override
    public IBooleanPrimtiveArray newBooleanArray(final String id, final int size) {
        Assertions.checkNull(finalizer.map.put(id, new BufferBooleanPrimitiveArray(
                new FakeAllocatorBuffer(nextId(), (BitSets.wordIndex(size - 1) + 1) * Long.BYTES), size)));
        final IBooleanPrimtiveArray array = (IBooleanPrimtiveArray) finalizer.map.get(id);
        Assertions.checkNotNull(array);
        clearBeforeUsage(array);
        return array;
    }

    @Override
    public IPrimitiveBitSet newBitSet(final String id, final int size) {
        final BufferBooleanPrimitiveArray booleanArray = (BufferBooleanPrimitiveArray) newBooleanArray(id, size);
        return booleanArray.getDelegate().getBitSet();
    }

    @Override
    public ILongPrimitiveArray newLongArray(final String id, final int size) {
        Assertions.checkNull(
                finalizer.map.put(id, new BufferLongPrimitiveArray(new FakeAllocatorBuffer(nextId(), size * Long.BYTES))));
        final ILongPrimitiveArray array = (ILongPrimitiveArray) finalizer.map.get(id);
        Assertions.checkNotNull(array);
        clearBeforeUsage(array);
        return array;
    }

    protected void clearBeforeUsage(final IPrimitiveArrayAccessor instance) {
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
