package de.invesdwin.context.persistence.timeseriesdb.array;

import java.io.Closeable;
import java.io.File;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.system.array.IPrimitiveArrayAllocator;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.collections.array.IBooleanArray;
import de.invesdwin.util.collections.array.IDoubleArray;
import de.invesdwin.util.collections.array.IIntegerArray;
import de.invesdwin.util.collections.array.ILongArray;
import de.invesdwin.util.collections.attributes.IAttributesMap;
import de.invesdwin.util.collections.bitset.IBitSet;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.lang.string.UniqueNameGenerator;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@ThreadSafe
public class TemporaryDiskPrimitiveArrayAllocator implements IPrimitiveArrayAllocator, Closeable {

    private static final UniqueNameGenerator UNIQUE_NAME_GENERATOR = new UniqueNameGenerator();
    private static final File DEFAULT_BASE_DIRECTORY = new File(ContextProperties.TEMP_DIRECTORY,
            TemporaryDiskPrimitiveArrayAllocator.class.getSimpleName());

    private final TemporaryDiskPrimitiveArrayAllocatorFinalizer finalizer;
    private final String name;
    private final File baseDirectory;

    public TemporaryDiskPrimitiveArrayAllocator(final String name) {
        this(name, DEFAULT_BASE_DIRECTORY);
    }

    public TemporaryDiskPrimitiveArrayAllocator(final String name, final File baseDirectory) {
        this.name = name;
        this.baseDirectory = baseDirectory;
        this.finalizer = new TemporaryDiskPrimitiveArrayAllocatorFinalizer();
        this.finalizer.register(this);
    }

    protected IPrimitiveArrayAllocator newDelegate(final String name, final File directory) {
        return new DiskPrimitiveArrayAllocator(name, directory);
    }

    protected IPrimitiveArrayAllocator getDelegate() {
        if (finalizer.delegate == null) {
            synchronized (this) {
                if (finalizer.delegate == null) {
                    final String uniqueName = UNIQUE_NAME_GENERATOR.get(name);
                    final File directory = new File(baseDirectory, uniqueName);
                    finalizer.delegate = newDelegate(uniqueName, directory);
                }
            }
        }
        return finalizer.delegate;
    }

    @Override
    public void close() {
        finalizer.close();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(final Class<T> type) {
        if (type.isAssignableFrom(getClass())) {
            return (T) this;
        } else {
            final IPrimitiveArrayAllocator delegateCopy = finalizer.delegate;
            if (delegateCopy != null) {
                delegateCopy.unwrap(type);
            }
            return null;
        }
    }

    @Override
    public IByteBuffer getByteBuffer(final String id) {
        return getDelegate().getByteBuffer(id);
    }

    @Override
    public IDoubleArray getDoubleArray(final String id) {
        return getDelegate().getDoubleArray(id);
    }

    @Override
    public IIntegerArray getIntegerArray(final String id) {
        return getDelegate().getIntegerArray(id);
    }

    @Override
    public IBooleanArray getBooleanArray(final String id) {
        return getDelegate().getBooleanArray(id);
    }

    @Override
    public IBitSet getBitSet(final String id) {
        return getDelegate().getBitSet(id);
    }

    @Override
    public ILongArray getLongArray(final String id) {
        return getDelegate().getLongArray(id);
    }

    @Override
    public IByteBuffer newByteBuffer(final String id, final int size) {
        return getDelegate().newByteBuffer(id, size);
    }

    @Override
    public IDoubleArray newDoubleArray(final String id, final int size) {
        return getDelegate().newDoubleArray(id, size);
    }

    @Override
    public IIntegerArray newIntegerArray(final String id, final int size) {
        return getDelegate().newIntegerArray(id, size);
    }

    @Override
    public IBooleanArray newBooleanArray(final String id, final int size) {
        return getDelegate().newBooleanArray(id, size);
    }

    @Override
    public IBitSet newBitSet(final String id, final int size) {
        return getDelegate().newBitSet(id, size);
    }

    @Override
    public ILongArray newLongArray(final String id, final int size) {
        return getDelegate().newLongArray(id, size);
    }

    @Override
    public IAttributesMap getAttributes() {
        return getDelegate().getAttributes();
    }

    @Override
    public IProperties getProperties() {
        return getDelegate().getProperties();
    }

    @Override
    public void clear() {
        getDelegate().clear();
    }

    @Override
    public boolean isOnHeap(final int size) {
        return getDelegate().isOnHeap(size);
    }

    @Override
    public File getDirectory() {
        return getDelegate().getDirectory();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(getDelegate()).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(DiskPrimitiveArrayAllocator.class, getDelegate());
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof TemporaryDiskPrimitiveArrayAllocator) {
            final TemporaryDiskPrimitiveArrayAllocator cObj = (TemporaryDiskPrimitiveArrayAllocator) obj;
            return Objects.equals(getDelegate(), cObj.finalizer.delegate);
        }
        return false;
    }

    private static final class TemporaryDiskPrimitiveArrayAllocatorFinalizer extends AFinalizer {

        private IPrimitiveArrayAllocator delegate;

        @Override
        protected void clean() {
            final IPrimitiveArrayAllocator delegateCopy = delegate;
            if (delegateCopy != null) {
                delegateCopy.clear();
                delegateCopy.close();
                delegate = null;
            }
        }

        @Override
        protected boolean isCleaned() {
            return delegate == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }
    }

}
