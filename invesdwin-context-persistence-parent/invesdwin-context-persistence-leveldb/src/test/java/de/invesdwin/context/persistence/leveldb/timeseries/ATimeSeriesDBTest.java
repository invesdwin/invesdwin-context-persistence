package de.invesdwin.context.persistence.leveldb.timeseries;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.leveldb.serde.ExtendedTypeDelegateSerde;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FDateBuilder;
import ezdb.serde.Serde;

// CHECKSTYLE:OFF
@NotThreadSafe
public class ATimeSeriesDBTest extends ATest {
    //CHECKSTYLE:ON

    @Test
    public void testGetPrevious() throws IncompleteUpdateFoundException {
        final String key = "asdf";
        final ATimeSeriesDB<String, FDate> table = new ATimeSeriesDB<String, FDate>("table") {

            @Override
            protected Serde<FDate> newValueSerde() {
                return new ExtendedTypeDelegateSerde<FDate>(FDate.class);
            }

            @Override
            protected Integer newFixedLength() {
                return null;
            }

            @Override
            protected String hashKeyToString(final String key) {
                return key;
            }

            @Override
            protected FDate extractTime(final FDate value) {
                return value;
            }

            @Override
            protected File getBaseDirectory() {
                return ContextProperties.TEMP_DIRECTORY;
            }
        };
        final List<FDate> dates = new ArrayList<>();
        for (int i = 2000; i <= 2010; i++) {
            dates.add(FDateBuilder.newDate(i));
        }
        new ATimeSeriesUpdater<String, FDate>(key, table) {

            @Override
            protected ICloseableIterable<? extends FDate> getSource(final FDate updateFrom) {
                return WrapperCloseableIterable.maybeWrap(dates);
            }

            @Override
            protected void onUpdateFinished(final Instant updateStart) {}

            @Override
            protected void onUpdateStart() {}

            @Override
            protected FDate extractTime(final FDate element) {
                return element;
            }

            @Override
            protected FDate extractEndTime(final FDate element) {
                return element;
            }

            @Override
            protected void onFlush(final int flushIndex, final Instant flushStart,
                    final ATimeSeriesUpdater<String, FDate>.UpdateProgress updateProgress) {

            }
        }.update();

        for (int i = 1; i < dates.size(); i++) {
            final FDate value = table.getPreviousValue(key, dates.get(dates.size() - 1), i);
            final FDate expectedValue = dates.get(dates.size() - i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 1; i < dates.size(); i++) {
            final FDate value = table.getPreviousValue(key, FDate.MAX_DATE, i);
            final FDate expectedValue = dates.get(dates.size() - i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }

    }

}
