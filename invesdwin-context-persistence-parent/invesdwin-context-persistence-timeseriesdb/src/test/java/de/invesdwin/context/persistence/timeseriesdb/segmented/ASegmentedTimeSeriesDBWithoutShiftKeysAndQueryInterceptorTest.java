package de.invesdwin.context.persistence.timeseriesdb.segmented;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.base.ABaseDBWithoutShiftKeysAndQueryInterceptorTest;
import de.invesdwin.context.persistence.timeseriesdb.segmented.ASegmentedTimeSeriesDBWithCacheTest.TestSegmentedTimeSeriesDB;
import de.invesdwin.util.time.date.FDate;

// CHECKSTYLE:OFF
@NotThreadSafe
public class ASegmentedTimeSeriesDBWithoutShiftKeysAndQueryInterceptorTest
        extends ABaseDBWithoutShiftKeysAndQueryInterceptorTest {
    //CHECKSTYLE:ON

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
