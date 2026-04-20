package de.invesdwin.context.persistence.timeseriesdb.array.large;

import java.io.Closeable;
import java.io.File;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.system.array.large.ILargeArrayAllocator;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.collections.array.large.IBooleanLargeArray;
import de.invesdwin.util.collections.array.large.IDoubleLargeArray;
import de.invesdwin.util.collections.array.large.IIntegerLargeArray;
import de.invesdwin.util.collections.array.large.ILongLargeArray;
import de.invesdwin.util.collections.array.large.bitset.ILargeBitSet;
import de.invesdwin.util.collections.attributes.IAttributesMap;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.lang.string.UniqueNameGenerator;
import de.invesdwin.util.streams.buffer.memory.IMemoryBuffer;

@ThreadSafe
public class TemporaryDiskLargeArrayAllocator implements ILargeArrayAllocator, Closeable {

    private static final UniqueNameGenerator UNIQUE_NAME_GENERATOR = new UniqueNameGenerator();
    private static final File DEFAULT_BASE_DIRECTORY = new File(ContextProperties.TEMP_DIRECTORY,
            TemporaryDiskLargeArrayAllocator.class.getSimpleName());

    private final TemporaryDiskLargeArrayAllocatorFinalizer finalizer;
    private final String uniqueName;
    private final File directory;

    public TemporaryDiskLargeArrayAllocator(final String name) {
        this(name, DEFAULT_BASE_DIRECTORY);
    }

    public TemporaryDiskLargeArrayAllocator(final String name, final File baseDirectory) {
        this.uniqueName = Files.normalizeFilename(UNIQUE_NAME_GENERATOR.get(name));
        this.directory = new File(baseDirectory, uniqueName);
        this.finalizer = new TemporaryDiskLargeArrayAllocatorFinalizer();
        this.finalizer.register(this);
    }

    protected ILargeArrayAllocator newDelegate(final String name, final File directory) {
        return new DiskLargeArrayAllocator(name, directory);
    }

    protected ILargeArrayAllocator getDelegate() {
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
            final ILargeArrayAllocator delegateCopy = finalizer.delegate;
            if (delegateCopy != null) {
                delegateCopy.unwrap(type);
            }
            return null;
        }
    }

    @Override
    public IMemoryBuffer getMemoryBuffer(final String id) {
        return getDelegate().getMemoryBuffer(id);
    }

    @Override
    public IDoubleLargeArray getDoubleArray(final String id) {
        return getDelegate().getDoubleArray(id);
    }

    @Override
    public IIntegerLargeArray getIntegerArray(final String id) {
        return getDelegate().getIntegerArray(id);
    }

    @Override
    public IBooleanLargeArray getBooleanArray(final String id) {
        return getDelegate().getBooleanArray(id);
    }

    @Override
    public ILargeBitSet getBitSet(final String id) {
        return getDelegate().getBitSet(id);
    }

    @Override
    public ILongLargeArray getLongArray(final String id) {
        return getDelegate().getLongArray(id);
    }

    @Override
    public IMemoryBuffer newMemoryBuffer(final String id, final long size) {
        return getDelegate().newMemoryBuffer(id, size);
    }

    @Override
    public IDoubleLargeArray newDoubleArray(final String id, final long size) {
        return getDelegate().newDoubleArray(id, size);
    }

    @Override
    public IIntegerLargeArray newIntegerArray(final String id, final long size) {
        return getDelegate().newIntegerArray(id, size);
    }

    @Override
    public IBooleanLargeArray newBooleanArray(final String id, final long size) {
        return getDelegate().newBooleanArray(id, size);
    }

    @Override
    public ILargeBitSet newBitSet(final String id, final long size) {
        return getDelegate().newBitSet(id, size);
    }

    @Override
    public ILongLargeArray newLongArray(final String id, final long size) {
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
    public boolean isOnHeap(final long size) {
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
        return Objects.hashCode(DiskLargeArrayAllocator.class, getDelegate());
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof TemporaryDiskLargeArrayAllocator) {
            final TemporaryDiskLargeArrayAllocator cObj = (TemporaryDiskLargeArrayAllocator) obj;
            return Objects.equals(getDelegate(), cObj.finalizer.delegate);
        }
        return false;
    }

    private static final class TemporaryDiskLargeArrayAllocatorFinalizer extends AFinalizer {

        private ILargeArrayAllocator delegate;

        @Override
        protected void clean() {
            final ILargeArrayAllocator delegateCopy = delegate;
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
