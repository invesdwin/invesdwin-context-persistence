package de.invesdwin.context.persistence.timeseriesdb.array;

import java.io.Closeable;
import java.io.File;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.system.array.primitive.IPrimitiveArrayAllocator;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.collections.array.primitive.IBooleanPrimtiveArray;
import de.invesdwin.util.collections.array.primitive.IDoublePrimitiveArray;
import de.invesdwin.util.collections.array.primitive.IIntegerPrimitiveArray;
import de.invesdwin.util.collections.array.primitive.ILongPrimitiveArray;
import de.invesdwin.util.collections.array.primitive.bitset.IPrimitiveBitSet;
import de.invesdwin.util.collections.attributes.IAttributesMap;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.lang.Files;
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
    private final String uniqueName;
    private final File directory;

    public TemporaryDiskPrimitiveArrayAllocator(final String name) {
        this(name, DEFAULT_BASE_DIRECTORY);
    }

    public TemporaryDiskPrimitiveArrayAllocator(final String name, final File baseDirectory) {
        this.uniqueName = Files.normalizeFilename(UNIQUE_NAME_GENERATOR.get(name));
        this.directory = new File(baseDirectory, uniqueName);
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
    public IDoublePrimitiveArray getDoubleArray(final String id) {
        return getDelegate().getDoubleArray(id);
    }

    @Override
    public IIntegerPrimitiveArray getIntegerArray(final String id) {
        return getDelegate().getIntegerArray(id);
    }

    @Override
    public IBooleanPrimtiveArray getBooleanArray(final String id) {
        return getDelegate().getBooleanArray(id);
    }

    @Override
    public IPrimitiveBitSet getBitSet(final String id) {
        return getDelegate().getBitSet(id);
    }

    @Override
    public ILongPrimitiveArray getLongArray(final String id) {
        return getDelegate().getLongArray(id);
    }

    @Override
    public IByteBuffer newByteBuffer(final String id, final int size) {
        return getDelegate().newByteBuffer(id, size);
    }

    @Override
    public IDoublePrimitiveArray newDoubleArray(final String id, final int size) {
        return getDelegate().newDoubleArray(id, size);
    }

    @Override
    public IIntegerPrimitiveArray newIntegerArray(final String id, final int size) {
        return getDelegate().newIntegerArray(id, size);
    }

    @Override
    public IBooleanPrimtiveArray newBooleanArray(final String id, final int size) {
        return getDelegate().newBooleanArray(id, size);
    }

    @Override
    public IPrimitiveBitSet newBitSet(final String id, final int size) {
        return getDelegate().newBitSet(id, size);
    }

    @Override
    public ILongPrimitiveArray newLongArray(final String id, final int size) {
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
    public ILock getLock(final String id) {
        return getDelegate().getLock(id);
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
