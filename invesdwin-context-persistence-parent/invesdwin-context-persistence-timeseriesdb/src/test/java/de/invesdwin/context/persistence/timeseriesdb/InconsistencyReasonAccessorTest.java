package de.invesdwin.context.persistence.timeseriesdb;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache.InconsistencyReason;
import de.invesdwin.context.persistence.timeseriesdb.delegate.ADelegateTimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.segmented.PeriodicalSegmentFinder;
import de.invesdwin.context.persistence.timeseriesdb.segmented.ASegmentedTimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.segmented.finder.HistoricalCacheSegmentFinder;
import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseriesdb.segmented.finder.ISegmentFinder;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.loadingcache.historical.AHistoricalCache;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.range.TimeRange;

/**
 * The reason a check recorded, readable through the table API rather than only through the cache.
 * <p>
 * {@code isEmptyOrInconsistent(K)} is the question every derived-store consumer asks, and every one of them answers it
 * with the same act - delete and re-fetch - because until now the WHY was not reachable from the interface: it lived on
 * the lookup cache. That is how an all-or-nothing recovery survives a refactor: the classification exists, the caller
 * cannot see it, so the caller wipes a key that was simply never built.
 * <p>
 * These cells pin the plumbing at each of the three store types plus the delegate wrapper, on states that need no
 * update machinery: a key nobody has built yet. The sound-store contrast (boolean false, reason NONE) is pinned in
 * {@code TimeSeriesInconsistencyReasonTest.testAHonestStoreRecordsNoReason}; the damaged-chain reasons are pinned in
 * the corrupted-generation and middle-segment cells of this module.
 */
@NotThreadSafe
public class InconsistencyReasonAccessorTest extends ATest {

    private File baseDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY, getClass().getSimpleName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Files.deleteQuietly(baseDirectory);
    }

    @Test
    public void testAPlainTableReportsWhichCheckIndictedIt() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newPlainTable("plainFresh");
        try {
            Assertions.checkTrue(table.isEmptyOrInconsistent("k"), "a key nobody has built is inconsistent-empty");
            Assertions.checkEquals(InconsistencyReason.NO_UPDATED_MARKER, table.getInconsistencyReason("k"),
                    "and the table API must say WHICH arm indicted it, not just that one did");
        } finally {
            table.close();
        }
    }

    @Test
    public void testADelegatingTablePassesItsTargetsReasonThrough() throws Exception {
        final ATimeSeriesDB<String, FDate> target = newPlainTable("delegateTarget");
        final ADelegateTimeSeriesDB<String, FDate> wrapper = new ADelegateTimeSeriesDB<String, FDate>() {

            @Override
            protected ITimeSeriesDB<String, FDate> newDelegate() {
                return target;
            }

        };
        try {
            Assertions.checkTrue(wrapper.isEmptyOrInconsistent("k"), "the delegate answers for the wrapped table");
            //without the explicit override this would fall back to the interface default and answer NONE - the same
            //output a healthy store gives, which is exactly how a wrapped table would silently lose its classification
            Assertions.checkEquals(InconsistencyReason.NO_UPDATED_MARKER, wrapper.getInconsistencyReason("k"),
                    "a delegating table must pass the wrapped store's reason through, not answer the default");
        } finally {
            wrapper.close();
        }
    }

    @Test
    public void testASegmentedTableReportsItsNeverBuiltCase() throws Exception {
        final ASegmentedTimeSeriesDB<String, FDate> table = newEmptySegmentedTable("segmentedFresh");
        try {
            Assertions.checkTrue(table.isEmptyOrInconsistent("k"),
                    "a segmented key with no COMPLETE segment is inconsistent-empty");
            Assertions.checkEquals(InconsistencyReason.NO_SUMMARIES, table.getInconsistencyReason("k"),
                    "and it must say so - the segmented check has no updated-marker file to consult, its equivalent is"
                            + " an empty status table, and a consumer needs to be able to tell 'nothing to wipe' from"
                            + " 'something is damaged'");
        } finally {
            table.close();
        }
    }

    private ATimeSeriesDB<String, FDate> newPlainTable(final String name) {
        return new ATimeSeriesDB<String, FDate>(name) {

            @Override
            protected ISerde<FDate> newValueSerde() {
                return FDateSerde.GET;
            }

            @Override
            protected Integer newValueFixedLength() {
                return FDateSerde.FIXED_LENGTH;
            }

            @Override
            public ITimeSeriesBaseDirectory getBaseDirectory() {
                return new TimeSeriesBaseDirectory(baseDirectory);
            }

            @Override
            public String innerHashKeyToString(final String key) {
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

        };
    }

    private ASegmentedTimeSeriesDB<String, FDate> newEmptySegmentedTable(final String name) {
        return new ASegmentedTimeSeriesDB<String, FDate>(name) {

            private final AHistoricalCache<TimeRange> segmentFinderCache = PeriodicalSegmentFinder
                    .newCache(new Duration(2, FTimeUnit.YEARS), false);
            private final ISegmentFinder segmentFinder = new HistoricalCacheSegmentFinder(segmentFinderCache, null);

            @Override
            public ISegmentFinder getSegmentFinder(final String key) {
                return segmentFinder;
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
            public String innerHashKeyToString(final String key) {
                return key;
            }

            @Override
            public ITimeSeriesBaseDirectory getBaseDirectory() {
                return new TimeSeriesBaseDirectory(baseDirectory);
            }

            @Override
            protected String getElementsName() {
                return "InconsistencyReasonAccessorTest";
            }

            @Override
            protected ICloseableIterable<? extends FDate> downloadSegmentElements(
                    final SegmentedKey<String> key) {
                return EmptyCloseableIterable.getInstance();
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
            public FDate getFirstAvailableHistoricalSegmentFrom(final String key) {
                return null;
            }

            @Override
            public FDate getLastAvailableHistoricalSegmentTo(final String key, final FDate updateTo) {
                return null;
            }

        };
    }

}
