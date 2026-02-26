package de.invesdwin.context.persistence.timeseriesdb.array;

import java.io.Closeable;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.system.array.IPrimitiveArrayAllocator;
import de.invesdwin.context.system.array.IPrimitiveArrayAllocatorFactory;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import de.invesdwin.util.lang.Objects;

@Immutable
public final class ThresholdDiskPrimitiveArrayAllocatorFactory implements IPrimitiveArrayAllocatorFactory, Closeable {

    @GuardedBy("this.class")
    private static final ALoadingCache<String, ThresholdDiskPrimitiveArrayAllocatorFactory> FACTORIES = new ALoadingCache<String, ThresholdDiskPrimitiveArrayAllocatorFactory>() {
        @Override
        protected ThresholdDiskPrimitiveArrayAllocatorFactory loadValue(final String key) {
            return new ThresholdDiskPrimitiveArrayAllocatorFactory(key);
        }
    };

    private final String name;

    @GuardedBy("this")
    private ThresholdDiskPrimitiveArrayAllocator allocator;

    private ThresholdDiskPrimitiveArrayAllocatorFactory(final String name) {
        this.name = name;
    }

    @Override
    public synchronized void close() {
        if (allocator != null) {
            allocator.clear();
            allocator.close();
            allocator = null;
        }
    }

    @Override
    public IPrimitiveArrayAllocator newInstance() {
        if (allocator == null) {
            synchronized (this) {
                if (allocator == null) {
                    allocator = new ThresholdDiskPrimitiveArrayAllocator(name);
                }
            }
        }
        return allocator;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(name).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(DiskPrimitiveArrayAllocator.class, name);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof ThresholdDiskPrimitiveArrayAllocatorFactory) {
            final ThresholdDiskPrimitiveArrayAllocatorFactory cObj = (ThresholdDiskPrimitiveArrayAllocatorFactory) obj;
            return Objects.equals(name, cObj.name);
        }
        return false;
    }

    public static synchronized IPrimitiveArrayAllocatorFactory getInstance(final String name) {
        return FACTORIES.get(name);
    }

    public static synchronized void clear() {
        if (!FACTORIES.isEmpty()) {
            for (final ThresholdDiskPrimitiveArrayAllocatorFactory factory : FACTORIES.values()) {
                factory.close();
            }
            FACTORIES.clear();
        }
    }

}
