package de.invesdwin.context.persistence.timeseriesdb.segmented;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.base.ANoGapValuesBaseDBWithCacheTest;
import de.invesdwin.context.persistence.timeseriesdb.segmented.ASegmentedTimeSeriesDBWithCacheTest.TestSegmentedTimeSeriesDB;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class NoGapValuesASegmentedTimeSeriesDBWithCacheTest extends ANoGapValuesBaseDBWithCacheTest {

    @Override
    protected void putNewEntity(final FDate newEntity) throws IncompleteUpdateRetryableException {
        //noop
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        table = new TestSegmentedTimeSeriesDB(getClass().getSimpleName(), entities);
    }

}
