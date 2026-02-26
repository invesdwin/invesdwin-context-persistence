package de.invesdwin.context.persistence.timeseriesdb.array;

import java.io.Closeable;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;

import org.jspecify.annotations.Nullable;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;

import de.invesdwin.context.system.array.IPrimitiveArrayAllocator;
import de.invesdwin.context.system.array.IPrimitiveArrayAllocatorFactory;
import de.invesdwin.util.lang.Objects;

@Immutable
public final class ThresholdDiskPrimitiveArrayAllocatorFactory implements IPrimitiveArrayAllocatorFactory, Closeable {

    /*
     * only evict factories when they are not used anymore, so we can clear everything between tests without worrying
     * about reference leaks between tests
     */
    @GuardedBy("this.class")
    private static final LoadingCache<String, ThresholdDiskPrimitiveArrayAllocatorFactory> FACTORIES = Caffeine
            .newBuilder()
            .weakValues()
            .removalListener(ThresholdDiskPrimitiveArrayAllocatorFactory::factories_onRemoval)
            .<String, ThresholdDiskPrimitiveArrayAllocatorFactory> build(
                    ThresholdDiskPrimitiveArrayAllocatorFactory::factories_load);

    private final String name;

    @GuardedBy("this")
    private ThresholdDiskPrimitiveArrayAllocator allocator;

    private ThresholdDiskPrimitiveArrayAllocatorFactory(final String name) {
        this.name = name;
    }

    private static void factories_onRemoval(@Nullable final String key,
            final ThresholdDiskPrimitiveArrayAllocatorFactory value, final RemovalCause removalCause) {
        value.close();
    }

    private static ThresholdDiskPrimitiveArrayAllocatorFactory factories_load(final String key) {
        return new ThresholdDiskPrimitiveArrayAllocatorFactory(key);
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
        for (final ThresholdDiskPrimitiveArrayAllocatorFactory factory : FACTORIES.asMap().values()) {
            factory.close();
        }
    }

}
