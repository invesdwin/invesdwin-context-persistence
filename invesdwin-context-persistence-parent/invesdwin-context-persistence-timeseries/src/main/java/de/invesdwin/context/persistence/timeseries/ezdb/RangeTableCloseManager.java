package de.invesdwin.context.persistence.timeseries.ezdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.ThreadSafe;

import com.github.benmanes.caffeine.cache.Caffeine;

import de.invesdwin.context.beans.hook.ReinitializationHookManager;
import de.invesdwin.context.beans.hook.ReinitializationHookSupport;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import de.invesdwin.util.collections.loadingcache.historical.AHistoricalCache;
import de.invesdwin.util.collections.loadingcache.historical.refresh.HistoricalCacheRefreshManager;

/**
 * This class makes sure that all range tables are closed on application reinitialization. This makes sure that no beans
 * keep open instances that collide with new instances being created during context restart. This is needed because
 * leveldb allows only one table instance accessing it which is enforced by a lock file.
 */
@ThreadSafe
public final class RangeTableCloseManager {

    private static final org.slf4j.ext.XLogger LOG = org.slf4j.ext.XLoggerFactory
            .getXLogger(HistoricalCacheRefreshManager.class);

    private static final ALoadingCache<String, Set<ADelegateRangeTable<?, ?, ?>>> REGISTERED_CACHES = new ALoadingCache<String, Set<ADelegateRangeTable<?, ?, ?>>>() {

        @Override
        protected Set<ADelegateRangeTable<?, ?, ?>> loadValue(final String key) {
            final ConcurrentMap<ADelegateRangeTable<?, ?, ?>, Boolean> map = Caffeine.newBuilder()
                    .weakKeys()
                    .<ADelegateRangeTable<?, ?, ?>, Boolean> build()
                    .asMap();
            return Collections.newSetFromMap(map);
        }

        @Override
        protected boolean isHighConcurrency() {
            return true;
        }
    };

    static {
        ReinitializationHookManager.register(new ReinitializationHookSupport() {
            @Override
            public void reinitializationStarted() {
                closeAll();
            }
        });
    }

    private RangeTableCloseManager() {}

    public static synchronized boolean register(final ADelegateRangeTable<?, ?, ?> cache) {
        final Set<ADelegateRangeTable<?, ?, ?>> caches = REGISTERED_CACHES.get(cache.toString());
        if (!caches.add(cache)) {
            return false;
        }
        final int size = caches.size();
        if (size % 10000 == 0) {
            //CHECKSTYLE:OFF
            LOG.warn(
                    "Already registered {} {}s, maybe the cache instance should be cached itself instead of being recreated all the time? Class={} ToString={}",
                    size, AHistoricalCache.class.getSimpleName(), cache.getClass().getSimpleName(), cache);
            //CHECKSTYLE:ON
        }
        return true;
    }

    public static synchronized void closeAll() {
        final List<ADelegateRangeTable<?, ?, ?>> cachesCopy = new ArrayList<>();
        for (final Set<ADelegateRangeTable<?, ?, ?>> caches : REGISTERED_CACHES.values()) {
            for (final ADelegateRangeTable<?, ?, ?> registeredCache : caches) {
                cachesCopy.add(registeredCache);
            }
        }
        for (final ADelegateRangeTable<?, ?, ?> cache : cachesCopy) {
            cache.close();
        }
    }

    public static synchronized boolean unregister(final ADelegateRangeTable<?, ?, ?> cache) {
        final Set<ADelegateRangeTable<?, ?, ?>> caches = REGISTERED_CACHES.get(cache.toString());
        return caches.remove(cache);
    }

}
