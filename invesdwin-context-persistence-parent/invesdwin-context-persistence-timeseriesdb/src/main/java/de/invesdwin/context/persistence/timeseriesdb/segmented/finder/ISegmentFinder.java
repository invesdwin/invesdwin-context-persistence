package de.invesdwin.context.persistence.timeseriesdb.segmented.finder;

import de.invesdwin.util.collections.loadingcache.historical.IHistoricalCache;
import de.invesdwin.util.collections.loadingcache.historical.query.IHistoricalCacheQuery;
import de.invesdwin.util.collections.loadingcache.historical.query.IHistoricalCacheQueryWithFuture;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.timezone.FTimeZone;
import de.invesdwin.util.time.range.TimeRange;

public interface ISegmentFinder {

    FTimeZone getOffsetTimeZone();

    FDate getDay(FDate time);

    IHistoricalCache<TimeRange> getCache();

    IHistoricalCacheQuery<TimeRange> getCacheQuery();

    IHistoricalCacheQueryWithFuture<TimeRange> getCacheQueryWithFuture();

    IHistoricalCacheQuery<TimeRange> getCacheQueryWithFutureNull();

}
