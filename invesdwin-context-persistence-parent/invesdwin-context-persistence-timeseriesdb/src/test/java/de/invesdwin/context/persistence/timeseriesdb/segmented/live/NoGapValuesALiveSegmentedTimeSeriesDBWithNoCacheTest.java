package de.invesdwin.context.persistence.timeseriesdb.segmented.live;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.base.ANoGapValuesBaseDBWithNoCacheTest;
import de.invesdwin.context.persistence.timeseriesdb.segmented.live.ALiveSegmentedTimeSeriesDBWithCacheTest.TestLiveSegmentedTimeSeriesDB;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class NoGapValuesALiveSegmentedTimeSeriesDBWithNoCacheTest extends ANoGapValuesBaseDBWithNoCacheTest {

    @Override
    protected void putNewEntity(final FDate newEntity) throws IncompleteUpdateRetryableException {
        ((ALiveSegmentedTimeSeriesDB<String, FDate>) table).putNextLiveValue(KEY, newEntity);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        table = new TestLiveSegmentedTimeSeriesDB(getClass().getSimpleName(), entities);
    }
}
