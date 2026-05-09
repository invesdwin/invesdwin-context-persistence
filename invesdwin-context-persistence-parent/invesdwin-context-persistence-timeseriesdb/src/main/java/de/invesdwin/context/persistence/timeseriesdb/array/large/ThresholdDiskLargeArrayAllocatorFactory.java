package de.invesdwin.context.persistence.timeseriesdb.array.large;

import java.io.Closeable;
import java.util.Map;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;

import de.invesdwin.context.system.array.large.ILargeArrayAllocator;
import de.invesdwin.context.system.array.large.ILargeArrayAllocatorFactory;
import de.invesdwin.util.lang.Objects;

@Immutable
public final class ThresholdDiskLargeArrayAllocatorFactory implements ILargeArrayAllocatorFactory, Closeable {

    /*
     * only evict factories when they are not used anymore, so we can clear everything between tests without worrying
     * about reference leaks between tests
     */
    @GuardedBy("this.class")
    private static final LoadingCache<String, ThresholdDiskLargeArrayAllocatorFactory> FACTORIES = Caffeine.newBuilder()
            .weakValues()
            .removalListener(ThresholdDiskLargeArrayAllocatorFactory::factories_onRemoval)
            .<String, ThresholdDiskLargeArrayAllocatorFactory> build(
                    ThresholdDiskLargeArrayAllocatorFactory::factories_load);

    private final String name;

    @GuardedBy("this")
    private ThresholdDiskLargeArrayAllocator allocator;

    private ThresholdDiskLargeArrayAllocatorFactory(final String name) {
        this.name = name;
    }

    private static void factories_onRemoval(final String key, final ThresholdDiskLargeArrayAllocatorFactory value,
            final RemovalCause removalCause) {
        if (value != null) {
            value.close();
        }
    }

    private static ThresholdDiskLargeArrayAllocatorFactory factories_load(final String key) {
        return new ThresholdDiskLargeArrayAllocatorFactory(key);
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
    public ILargeArrayAllocator newInstance() {
        if (allocator == null) {
            synchronized (this) {
                if (allocator == null) {
                    allocator = new ThresholdDiskLargeArrayAllocator(name);
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
        return Objects.hashCode(DiskLargeArrayAllocator.class, name);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof ThresholdDiskLargeArrayAllocatorFactory) {
            final ThresholdDiskLargeArrayAllocatorFactory cObj = (ThresholdDiskLargeArrayAllocatorFactory) obj;
            return Objects.equals(name, cObj.name);
        }
        return false;
    }

    public static synchronized ILargeArrayAllocatorFactory getInstance(final String name) {
        return FACTORIES.get(name);
    }

    public static synchronized void clear() {
        final Map<String, ThresholdDiskLargeArrayAllocatorFactory> map = FACTORIES.asMap();
        if (!map.isEmpty()) {
            for (final ThresholdDiskLargeArrayAllocatorFactory factory : map.values()) {
                factory.close();
            }
        }
    }

}
