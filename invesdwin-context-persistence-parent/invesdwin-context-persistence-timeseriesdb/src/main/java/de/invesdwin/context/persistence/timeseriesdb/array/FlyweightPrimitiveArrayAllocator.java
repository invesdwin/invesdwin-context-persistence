package de.invesdwin.context.persistence.timeseriesdb.array;

import java.io.Closeable;
import java.io.File;

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
import de.invesdwin.util.collections.array.buffer.BufferBooleanArray;
import de.invesdwin.util.collections.array.buffer.BufferDoubleArray;
import de.invesdwin.util.collections.array.buffer.BufferIntegerArray;
import de.invesdwin.util.collections.array.buffer.BufferLongArray;
import de.invesdwin.util.collections.attributes.AttributesMap;
import de.invesdwin.util.collections.attributes.IAttributesMap;
import de.invesdwin.util.collections.bitset.IBitSet;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.math.BitSets;
import de.invesdwin.util.streams.buffer.bytes.FakeAllocatorBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@ThreadSafe
public class FlyweightPrimitiveArrayAllocator implements IPrimitiveArrayAllocator, Closeable {

    private final FlyweightPrimitiveArrayPersistentMap<String> map;
    private AttributesMap attributes;
    private IProperties properties;

    public FlyweightPrimitiveArrayAllocator(final String name, final File directory) {
        this.map = new FlyweightPrimitiveArrayPersistentMap<>(name, directory);
    }

    public File getDirectory() {
        return map.getDirectory();
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
        Assertions.checkNull(map.put(id,
                new BufferBooleanArray(new FakeAllocatorBuffer((BitSets.wordIndex(size) + 1) * Long.BYTES), size)));
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

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(getDirectory()).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(FlyweightPrimitiveArrayAllocator.class, getDirectory());
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof FlyweightPrimitiveArrayAllocator) {
            final FlyweightPrimitiveArrayAllocator cObj = (FlyweightPrimitiveArrayAllocator) obj;
            return Objects.equals(getDirectory(), cObj.getDirectory());
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
        if (attributes == null) {
            synchronized (this) {
                if (attributes == null) {
                    attributes = new AttributesMap();
                }
            }
        }
        return attributes;
    }

    @Override
    public IProperties getProperties() {
        if (properties == null) {
            synchronized (this) {
                if (properties == null) {
                    properties = new CachingDelegateProperties(new FileProperties(newPropertiesFile()));
                }
            }
        }
        return properties;
    }

    private File newPropertiesFile() {
        return new File(getDirectory(), map.getName() + ".properties");
    }

    @Override
    public void clear() {
        map.clear();
        final AttributesMap attributesCopy = attributes;
        if (attributesCopy != null) {
            attributesCopy.clear();
            attributes = null;
        }
        properties = null;
    }

    @Override
    public void close() {
        map.close();
    }

}
