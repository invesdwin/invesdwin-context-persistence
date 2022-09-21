package de.invesdwin.context.persistence.timeseriesdb.segmented.finder;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.segmented.PeriodicalSegmentFinder;
import de.invesdwin.util.collections.loadingcache.historical.AHistoricalCache;
import de.invesdwin.util.collections.loadingcache.historical.IHistoricalCache;
import de.invesdwin.util.collections.loadingcache.historical.query.IHistoricalCacheQuery;
import de.invesdwin.util.collections.loadingcache.historical.query.IHistoricalCacheQueryWithFuture;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.timezone.FTimeZone;
import de.invesdwin.util.time.range.TimeRange;

@ThreadSafe
public final class DummySegmentFinder implements ISegmentFinder {

    public static final DummySegmentFinder INSTANCE = new DummySegmentFinder();
    private final AHistoricalCache<TimeRange> cache;
    private final IHistoricalCacheQuery<TimeRange> cacheQuery;
    private final IHistoricalCacheQueryWithFuture<TimeRange> cacheQueryWithFuture;
    private final IHistoricalCacheQuery<TimeRange> cacheQueryWithFutureNull;

    private DummySegmentFinder() {
        this.cache = PeriodicalSegmentFinder.DUMMY_CACHE;
        this.cacheQuery = PeriodicalSegmentFinder.DUMMY_CACHE.query();
        this.cacheQueryWithFuture = PeriodicalSegmentFinder.DUMMY_CACHE.query().setFutureEnabled();
        this.cacheQueryWithFutureNull = PeriodicalSegmentFinder.DUMMY_CACHE.query().setFutureNullEnabled();
    }

    @Override
    public FTimeZone getOffsetTimeZone() {
        return null;
    }

    @Override
    public FDate getDay(final FDate time) {
        return time;
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
