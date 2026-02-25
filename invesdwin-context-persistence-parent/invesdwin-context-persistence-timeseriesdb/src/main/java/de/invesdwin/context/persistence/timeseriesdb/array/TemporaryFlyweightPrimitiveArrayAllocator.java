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
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.lang.string.UniqueNameGenerator;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.closeable.Closeables;

@ThreadSafe
public class TemporaryFlyweightPrimitiveArrayAllocator implements IPrimitiveArrayAllocator, Closeable {

    private static final UniqueNameGenerator UNIQUE_NAME_GENERATOR = new UniqueNameGenerator();
    private static final File DEFAULT_BASE_DIRECTORY = new File(ContextProperties.TEMP_DIRECTORY,
            TemporaryFlyweightPrimitiveArrayAllocator.class.getSimpleName());

    private final TemporaryFlyweightPrimitiveArrayAllocatorFinalizer finalizer;

    public TemporaryFlyweightPrimitiveArrayAllocator(final String name) {
        this(name, DEFAULT_BASE_DIRECTORY);
    }

    public TemporaryFlyweightPrimitiveArrayAllocator(final String name, final File baseDirectory) {
        final String uniqueName = UNIQUE_NAME_GENERATOR.get(name);
        final File directory = new File(baseDirectory, uniqueName);
        this.finalizer = new TemporaryFlyweightPrimitiveArrayAllocatorFinalizer(newDelegate(uniqueName, directory));
        this.finalizer.register(this);
    }

    protected IPrimitiveArrayAllocator newDelegate(final String name, final File directory) {
        return new FlyweightPrimitiveArrayAllocator(name, directory);
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
            return getDelegate().unwrap(type);
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

    private IPrimitiveArrayAllocator getDelegate() {
        return finalizer.delegate;
    }

    private static final class TemporaryFlyweightPrimitiveArrayAllocatorFinalizer extends AFinalizer {

        private IPrimitiveArrayAllocator delegate;

        private TemporaryFlyweightPrimitiveArrayAllocatorFinalizer(final IPrimitiveArrayAllocator delegate) {
            this.delegate = delegate;
        }

        @Override
        protected void clean() {
            final IPrimitiveArrayAllocator delegateCopy = delegate;
            if (delegateCopy != null) {
                delegateCopy.clear();
                Closeables.close(delegateCopy);
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
