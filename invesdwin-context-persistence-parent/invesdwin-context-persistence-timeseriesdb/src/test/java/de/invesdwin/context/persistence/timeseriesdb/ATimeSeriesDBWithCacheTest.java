package de.invesdwin.context.persistence.timeseriesdb;

import java.io.File;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.base.ABaseDBWithCacheTest;
import de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.IUpdateProgress;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.collections.iterable.skip.ASkippingIterable;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;

// CHECKSTYLE:OFF
@NotThreadSafe
public class ATimeSeriesDBWithCacheTest extends ABaseDBWithCacheTest {
    //CHECKSTYLE:ON

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

    public static final class TestTimeSeriesUpdater extends ATimeSeriesUpdater<String, FDate> {
        private final List<FDate> entities;

        public TestTimeSeriesUpdater(final String key, final ITimeSeriesDBInternals<String, FDate> table,
                final List<FDate> entities) {
            super(key, table);
            this.entities = entities;
        }

        @Override
        protected ICloseableIterable<? extends FDate> getSource(final FDate updateFrom) {
            return new ASkippingIterable<FDate>(WrapperCloseableIterable.maybeWrap(entities)) {
                @Override
                protected boolean skip(final FDate element) {
                    return element.isBefore(updateFrom);
                }
            };
        }

        @Override
        protected void onUpdateFinished(final Instant updateStart) {}

        @Override
        protected void onUpdateStart() {}

        @Override
        protected FDate extractStartTime(final FDate element) {
            return element;
        }

        @Override
        protected FDate extractEndTime(final FDate element) {
            return element;
        }

        @Override
        protected void onElement(final IUpdateProgress<String, FDate> updateProgress) {}

        @Override
        protected void onFlush(final int flushIndex, final IUpdateProgress<String, FDate> updateProgress) {}

        @Override
        public Percent getProgress(final FDate minTime, final FDate maxTime) {
            return null;
        }
    }

    public static final class TestTimeSeriesDB extends ATimeSeriesDB<String, FDate> {
        public TestTimeSeriesDB(final String name) {
            super(name);
        }

        @Override
        protected ISerde<FDate> newValueSerde() {
            return FDateSerde.GET;
        }

        @Override
        protected Integer newValueFixedLength() {
            return FDateSerde.FIXED_LENGTH;
        }

        @Override
        protected String innerHashKeyToString(final String key) {
            return key;
        }

        @Override
        public FDate extractStartTime(final FDate value) {
            return value;
        }

        @Override
        public FDate extractEndTime(final FDate value) {
            return value;
        }

        @Override
        public File getBaseDirectory() {
            return ContextProperties.TEMP_DIRECTORY;
        }
    }

}
