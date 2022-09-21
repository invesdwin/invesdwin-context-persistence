package de.invesdwin.context.persistence.timeseriesdb.segmented.finder;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.collections.loadingcache.historical.IHistoricalCache;
import de.invesdwin.util.collections.loadingcache.historical.query.IHistoricalCacheQuery;
import de.invesdwin.util.collections.loadingcache.historical.query.IHistoricalCacheQueryWithFuture;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.timezone.FTimeZone;
import de.invesdwin.util.time.range.TimeRange;

@ThreadSafe
public class HistoricalCacheSegmentFinder implements ISegmentFinder {

    private final IHistoricalCache<TimeRange> cache;
    private final IHistoricalCacheQuery<TimeRange> cacheQuery;
    private final IHistoricalCacheQueryWithFuture<TimeRange> cacheQueryWithFuture;
    private final IHistoricalCacheQuery<TimeRange> cacheQueryWithFutureNull;
    private final FTimeZone offsetTimeZone;

    public HistoricalCacheSegmentFinder(final IHistoricalCache<TimeRange> cache, final FTimeZone offsetTimeZone) {
        this.cache = cache;
        this.cacheQuery = cache.query();
        this.cacheQueryWithFuture = cache.query().setFutureEnabled();
        this.cacheQueryWithFutureNull = cache.query().setFutureNullEnabled();
        this.offsetTimeZone = offsetTimeZone;
    }

    @Override
    public FTimeZone getOffsetTimeZone() {
        return offsetTimeZone;
    }

    @Override
    public FDate getDay(final FDate time) {
        if (time == null) {
            return null;
        }
        if (offsetTimeZone == null) {
            return time.withoutTime();
        } else {
            return time.withoutTime(offsetTimeZone);
        }
    }

    @Override
    public IHistoricalCache<TimeRange> getCache() {
        return cache;
    }

    @Override
    public IHistoricalCacheQuery<TimeRange> getCacheQuery() {
        return cacheQuery;
    }

    @Override
    public IHistoricalCacheQueryWithFuture<TimeRange> getCacheQueryWithFuture() {
        return cacheQueryWithFuture;
    }

    @Override
    public IHistoricalCacheQuery<TimeRange> getCacheQueryWithFutureNull() {
        return cacheQueryWithFutureNull;
    }

}
