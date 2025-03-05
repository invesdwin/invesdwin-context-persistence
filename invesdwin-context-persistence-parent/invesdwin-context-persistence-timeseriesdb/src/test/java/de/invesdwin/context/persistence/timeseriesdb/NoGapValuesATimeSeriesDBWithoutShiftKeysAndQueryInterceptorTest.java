package de.invesdwin.context.persistence.timeseriesdb;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDBWithCacheTest.TestTimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDBWithCacheTest.TestTimeSeriesUpdater;
import de.invesdwin.context.persistence.timeseriesdb.base.ANoGapValuesBaseDBWithoutShiftKeysAndQueryInterceptorTest;
import de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class NoGapValuesATimeSeriesDBWithoutShiftKeysAndQueryInterceptorTest
        extends ANoGapValuesBaseDBWithoutShiftKeysAndQueryInterceptorTest {

    private ATimeSeriesUpdater<String, FDate> updater;

    @Override
    protected void putNewEntity(final FDate newEntity) throws IncompleteUpdateRetryableException {
        updater.update();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        table = new TestTimeSeriesDB(getClass().getSimpleName());
        updater = new TestTimeSeriesUpdater(KEY, (ATimeSeriesDB<String, FDate>) table, entities);
        updater.update();
    }

}
