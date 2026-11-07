package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdateProgress;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.Longs;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FDates;

/**
 * A redo of the standalone '_incomplete_' last file drops that file's last value (it might be an incomplete bar) and
 * expects the source to hand it back. That expectation is not a guarantee - a source whose advertised end stops short
 * of that position never answers it - and then the redo used to DELETE a value the store had already published and
 * validated. Doubt about a value is not grounds for dropping it: a published value is only replaced by a better answer
 * for the same position, never by the absence of one.
 * <p>
 * So the replacement pass re-publishes the dropped value as-is whenever the source does not serve that position again
 * (and keeps the source's answer when it does). The published chain becomes a function of the previous generation plus
 * what the source answered, and stops drifting with attempt order - the live signature this pins is the tail of a
 * derived-bars key moving 2025-01-31T20:59 -> 20:58 -> 20:59 -> 20:57 across four consecutive attempts of the same
 * key, with the byte high-water following it down.
 */
@NotThreadSafe
public class TimeSeriesRedoTailPreservationTest extends ATest {

    private static final int BATCH_FLUSH_INTERVAL = 4;
    private static final String KEY = "redoTailKey";

    private File baseDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY,
                TimeSeriesRedoTailPreservationTest.class.getSimpleName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Files.deleteQuietly(baseDirectory);
    }

    /**
     * The series-20 shape: attempt N's supply serves the tail value, attempt N+1's supply stops one value short of it.
     * The published content must not change between them - the tail is re-published from the published state - and the
     * chain's value count and byte high-water must not creep down.
     */
    @Test
    public void testRedoRepublishesTheTailTheSourceNoLongerServes() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("redoTailShort");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final List<FDate> firstPass = newValues(1, BATCH_FLUSH_INTERVAL + 2);
        newUpdater(table, firstPass).update().close();

        final List<FDate> publishedFirst = readValues(table, KEY);
        final long highWaterFirst = chainHighWater(lookup);
        Assertions.assertThat(publishedFirst).as("pass 1 content").isEqualTo(new ArrayList<>(firstPass));

        //pass 2: the supply no longer reaches the value the redo drops
        newUpdater(table, firstPass.subList(0, firstPass.size() - 1)).update().close();

        Assertions.assertThat(readValues(table, KEY)).as("content after a redo whose supply stops short of the tail")
                .isEqualTo(new ArrayList<>(firstPass));
        Assertions.assertThat(chainHighWater(lookup)).as("byte high-water must not creep down")
                .isEqualTo(highWaterFirst);
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(), "the store must stay consistent: "
                + readPublishedSummaries(lookup));
    }

    /**
     * Repeating the short-supply attempt must not move the tail either - the published chain is a function of the
     * previous generation plus the source answer, not of how many attempts ran or in which order.
     */
    @Test
    public void testTailStaysStableAcrossRepeatedRedoAttempts() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("redoTailRepeat");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final List<FDate> full = newValues(1, BATCH_FLUSH_INTERVAL + 2);
        final List<FDate> shortSupply = full.subList(0, full.size() - 1);
        newUpdater(table, full).update().close();
        final long highWaterFirst = chainHighWater(lookup);

        for (int attempt = 1; attempt <= 3; attempt++) {
            //alternating supply answers: short, complete, short again
            final List<FDate> supply = attempt % 2 == 1 ? shortSupply : full;
            newUpdater(table, supply).update().close();
            Assertions.assertThat(readValues(table, KEY)).as("content after attempt " + attempt)
                    .isEqualTo(new ArrayList<>(full));
            Assertions.assertThat(chainHighWater(lookup)).as("byte high-water after attempt " + attempt)
                    .isEqualTo(highWaterFirst);
        }
    }

    /**
     * When the source does answer that position again, its answer is the one that gets written - the re-published tail
     * must not duplicate or shadow it.
     */
    @Test
    public void testSourceAnswerForTheTailPositionWinsWithoutDuplicating() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("redoTailServed");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final List<FDate> firstPass = newValues(1, BATCH_FLUSH_INTERVAL + 2);
        newUpdater(table, firstPass).update().close();
        final long highWaterFirst = chainHighWater(lookup);

        //pass 2: the redo's dropped tail comes back from the source, plus a new value behind it
        final List<FDate> secondPass = newValues(1, BATCH_FLUSH_INTERVAL + 3);
        newUpdater(table, secondPass).update().close();

        final List<FDate> content = readValues(table, KEY);
        Assertions.assertThat(content).as("content must be the source answer, each value exactly once")
                .isEqualTo(new ArrayList<>(secondPass));
        Assertions.assertThat(chainHighWater(lookup)).as("byte high-water must grow with the extra value")
                .isGreaterThan(highWaterFirst);
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(), "the store must stay consistent: "
                + readPublishedSummaries(lookup));
    }

    /**
     * A source that answers only positions BEHIND the dropped tail (it skipped the tail itself) must not push the
     * published chain out of order: the re-published tail goes between the values the redo re-wrote and the later
     * answers.
     */
    @Test
    public void testRedoReinsertsTailBeforeLaterSourceAnswers() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("redoTailGap");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final List<FDate> firstPass = newValues(1, BATCH_FLUSH_INTERVAL + 2);
        newUpdater(table, firstPass).update().close();

        //pass 2: the supply stops short of the tail and then answers a later position
        final List<FDate> secondPass = new ArrayList<>(firstPass.subList(0, firstPass.size() - 1));
        secondPass.addAll(newValues(BATCH_FLUSH_INTERVAL + 5, BATCH_FLUSH_INTERVAL + 6));
        newUpdater(table, secondPass).update().close();

        final List<FDate> expected = new ArrayList<>(firstPass);
        expected.addAll(newValues(BATCH_FLUSH_INTERVAL + 5, BATCH_FLUSH_INTERVAL + 6));
        final List<FDate> content = readValues(table, KEY);
        Assertions.assertThat(content).as("the re-published tail must sit in time order before the later answers")
                .isEqualTo(expected);
        for (int i = 1; i < content.size(); i++) {
            Assertions.checkTrue(content.get(i - 1).isBeforeNotNullSafe(content.get(i)),
                    "the chain must stay strictly ascending: " + content);
        }
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(), "the store must stay consistent: "
                + readPublishedSummaries(lookup));
    }

    private static long chainHighWater(final TimeSeriesLookupStorageCache<String, FDate> lookup) {
        long highest = 0L;
        for (final MemoryFileSummary summary : readPublishedSummaries(lookup)) {
            highest = Longs.max(highest, summary.getPrecedingMemoryOffset() + summary.getMemoryOffset()
                    + summary.getMemoryLength());
        }
        return highest;
    }

    private static List<FDate> newValues(final int fromDay, final int toDay) {
        final List<FDate> values = new ArrayList<>();
        for (int day = fromDay; day <= toDay; day++) {
            values.add(FDateBuilder.newDate(2020, 1, 1).addDays(day));
        }
        return values;
    }

    private static List<MemoryFileSummary> readPublishedSummaries(
            final TimeSeriesLookupStorageCache<String, FDate> lookupTable) {
        final List<MemoryFileSummary> summaries = new ArrayList<>();
        try (ICloseableIterator<MemoryFileSummary> it = lookupTable.getMemoryFileLookupTable().range()) {
            Lists.toListWithoutHasNext(it, summaries);
        }
        return summaries;
    }

    private static List<FDate> readValues(final ATimeSeriesDB<String, FDate> table, final String key) {
        final List<FDate> values = new ArrayList<>();
        try (ICloseableIterator<FDate> it = table.rangeValues(key, FDates.MIN_DATE, FDates.MAX_DATE).iterator()) {
            Lists.toListWithoutHasNext(it, values);
        }
        return values;
    }

    private ATimeSeriesDB<String, FDate> newTable(final String name) {
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
            protected int newBatchFlushInterval() {
                return BATCH_FLUSH_INTERVAL;
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

    private ATimeSeriesUpdater<String, FDate> newUpdater(final ATimeSeriesDB<String, FDate> table,
            final List<FDate> values) {
        return new ATimeSeriesUpdater<String, FDate>(KEY, table) {

            @Override
            protected ICloseableIterable<? extends FDate> getSource(final FDate updateFrom) {
                return WrapperCloseableIterable.maybeWrap(values);
            }

            @Override
            protected void onUpdateStarted(final FDate updateStart) {}

            @Override
            protected void onUpdateFinished() {}

            @Override
            protected FDate extractStartTime(final FDate element) {
                return element;
            }

            @Override
            protected FDate extractEndTime(final FDate element) {
                return element;
            }

            @Override
            protected void onElement(final ITimeSeriesUpdateProgress relativeProgress, final long relativeCount) {}

            @Override
            protected void onFlush(final ITimeSeriesUpdateProgress relativeProgress, final long flushIndex) {}

            @Override
            public Percent getProgress(final FDate minTime, final FDate maxTime) {
                return null;
            }
        };
    }

}
