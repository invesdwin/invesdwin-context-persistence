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
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFiles;
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
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.date.FTimeUnit;

/**
 * The one value a redo is allowed to lose. {@link TimeSeriesResumableUpdateAttemptTest} pins the resume half of what a
 * successor attempt inherits - a published frontier is continued, not replayed, and re-offered values are not written
 * twice - so this class looks only at the other branch of {@code newUpdateTransaction}: the redo of a standalone
 * {@code '_incomplete_'} shard, which re-reads that shard, DROPS its last value and expects the source to answer that
 * position again. A source whose coverage has moved past it cannot, and the value is gone from the republished chain
 * while the store reads as perfectly consistent.
 * <p>
 * The fleet ruling on that state is that supply silence belongs to the verdict layer, not to a store-side immutability
 * guard (the redo tail preserving mechanism was removed for exactly this reason), so this lane adds observation only:
 * one WARN and one accessor naming the position that could not be re-asked. Nothing here prevents the loss.
 * <ol>
 * <li>the predicate, as a truth table;</li>
 * <li>a redo whose source still answers the dropped position: no report, and a hole further along the source's own
 * series is not read as one either (only the first value the attempt takes counts);</li>
 * <li>a RESUME over a completed boundary: nothing was dropped, so a source starting later than the frontier loses
 * nothing - this pins the branch condition of the report;</li>
 * <li>a redo whose source offers the whole range again but skips exactly the dropped position: reported, and the hole
 * is visible in the chain. The generous source is the point - a check placed on the first value the source is HANDS
 * rather than the first it TAKES would have looked at value 1 here and stayed silent.</li>
 * </ol>
 * Each scenario has its own store directory; nothing here touches a deployment's store.
 */
@NotThreadSafe
public class TimeSeriesRedoCoverageTest extends ATest {

    private static final int BATCH_FLUSH_INTERVAL = 4;
    private static final String KEY = "redoCoverageKey";

    private File baseDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY, TimeSeriesRedoCoverageTest.class.getSimpleName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Files.deleteQuietly(baseDirectory);
    }

    /**
     * Scenario 1: the predicate alone.
     */
    @Test
    public void testThePredicateFlagsOnlyAFirstTakenValueThatStartsLater() {
        final FDate dropped = FDateBuilder.newDate(2026, 11, 20, 12, 0, 0);
        Assertions.assertThat(ATimeSeriesUpdater.redoCoverageMissing(dropped, dropped))
                .as("the healthy redo re-answers the dropped position itself").isFalse();
        Assertions.assertThat(ATimeSeriesUpdater.redoCoverageMissing(dropped,
                dropped.add(FTimeUnit.MINUTES, 1L)))
                .as("a first value taken later means the source no longer serves the position").isTrue();
        Assertions.assertThat(ATimeSeriesUpdater.redoCoverageMissing(dropped,
                dropped.add(FTimeUnit.MINUTES, -1L)))
                .as("a first value reaching further back than the position is not a gap").isFalse();
        Assertions.assertThat(ATimeSeriesUpdater.redoCoverageMissing(null, dropped))
                .as("no redo, nothing to check").isFalse();
        Assertions.assertThat(ATimeSeriesUpdater.redoCoverageMissing(dropped, null))
                .as("answered elsewhere - the caller decides that shape, not the predicate").isFalse();
    }

    /**
     * Scenario 2: the healthy redo. A pass that redoes its last file ends on an {@code '_incomplete_'} shard, the
     * successor drops its last value, and a source still covering that position brings it back. A hole later in the
     * source's own series is the series' business and must not be read as a loss.
     */
    @Test
    public void testRedoOfAnIncompleteTailReAsksTheDroppedPositionWhenTheSourceStillServesIt() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("redoTailCovered");
        final List<FDate> first = newValues(1, 6);
        newUpdater(table, first, true).update().close();
        assertTailNamed(table, true);

        //the source re-offers 1..4 as duplicates, answers the dropped 6, and has nothing for 7
        final List<FDate> successorValues = new ArrayList<>(newValues(1, 6));
        successorValues.add(FDateBuilder.newDate(2026, 1, 8));
        final ATimeSeriesUpdater<String, FDate> successor = newUpdater(table, successorValues, true);
        successor.update().close();

        Assertions.assertThat(successor.getLastRedoCoverageGap())
                .as("the dropped position came back, so a hole further along is not this report's business").isNull();
        final List<FDate> expected = new ArrayList<>(newValues(1, 6));
        expected.add(FDateBuilder.newDate(2026, 1, 8));
        Assertions.assertThat(readValues(table))
                .as("the republished chain holds the series once, with the source's own hole intact")
                .isEqualTo(expected);
        Assertions.checkFalse(table.getLookupTableCache(KEY).isEmptyOrInconsistent(),
                "a healthy redo must not leave the key inconsistent");
    }

    /**
     * Scenario 3: the report belongs to the redo branch only. A resume over a completed boundary is asked from the
     * stored frontier too, and a source that starts later than that frontier has simply nothing new - no value was
     * dropped, so no value was lost. Without the branch condition this is a false accusation.
     */
    @Test
    public void testAResumeOverACompletedBoundaryNeverReportsAGap() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("resumeNoFalseGap");
        final List<FDate> first = newValues(1, 6);
        //shouldRedoLastFile false: the pass ends on a complete file, which is what a boundary-committing attempt that
        //died mid-write leaves its successor
        newUpdater(table, first, false).update().close();
        assertTailNamed(table, false);

        final List<FDate> successorValues = newValues(7, 10);
        final ATimeSeriesUpdater<String, FDate> successor = newUpdater(table, successorValues, true);
        successor.update().close();

        Assertions.assertThat(successor.getLastRedoCoverageGap())
                .as("a resume drops no value, so it can lose none").isNull();
        final List<FDate> expected = new ArrayList<>(first);
        expected.addAll(successorValues);
        Assertions.assertThat(readValues(table)).as("the resumed chain continues above the frontier").isEqualTo(expected);
    }

    /**
     * Scenario 4: the loss. The same redo, with a source that hands out the whole range again yet cannot answer the
     * dropped position: the chain comes back with a hole exactly there, the store reports itself consistent, and the
     * gap is the only thing that says so.
     */
    @Test
    public void testAGenerousSourceThatCannotAnswerTheDroppedPositionIsReported() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("redoTailGone");
        final List<FDate> first = newValues(1, 6);
        newUpdater(table, first, true).update().close();
        assertTailNamed(table, true);
        final FDate dropped = first.get(first.size() - 1);

        //generous but stale: 1..5 again (all duplicates to be skipped), then 7 onward - never 6
        final List<FDate> successorValues = new ArrayList<>(newValues(1, 5));
        successorValues.addAll(newValues(7, 10));
        final ATimeSeriesUpdater<String, FDate> successor = newUpdater(table, successorValues, true);
        successor.update().close();

        Assertions.assertThat(successor.getLastRedoCoverageGap())
                .as("the dropped position must be reported when the source cannot answer it").isEqualTo(dropped);
        final List<FDate> expected = new ArrayList<>(newValues(1, 5));
        expected.addAll(newValues(7, 10));
        final List<FDate> chain = readValues(table);
        Assertions.assertThat(chain).as("the hole is in the series, the layout stays sound").isEqualTo(expected);
        Assertions.assertThat(chain).doesNotContain(dropped);
        Assertions.checkFalse(table.getLookupTableCache(KEY).isEmptyOrInconsistent(),
                "no consistency check sees this loss - which is why the report exists");
    }

    // --- helpers ---

    private void assertTailNamed(final ATimeSeriesDB<String, FDate> table, final boolean incomplete) {
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final List<MemoryFileSummary> published = readPublishedSummaries(lookup);
        Assertions.checkTrue(!published.isEmpty(), "the pass must publish a chain");
        final MemoryFileSummary tail = published.get(published.size() - 1);
        final boolean isIncomplete = MemoryFiles.isIncompleteMemoryFile(tail.getMemoryResourceUri());
        Assertions.checkTrue(isIncomplete == incomplete,
                "precondition: the chain must end on "
                        + (incomplete ? "an '_incomplete_' shard, which is what a redo-able pass ends on (its last"
                                + " chunk has no following chunk to make it complete)"
                                : "a complete file, which is what a pass that does not redo its last file ends on")
                        + ": " + tail.getMemoryResourceUri());
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

    /**
     * @param redoLastFile the answer of {@link ATimeSeriesUpdater#shouldRedoLastFile()}, and so which branch of
     *            {@code newUpdateTransaction} the attempt takes. The source is handed out whole, as the real demand
     *            sources are: values at or below the store's frontier arrive as duplicates and the attempt's own
     *            skipping is what keeps them out of the chain.
     */
    private ATimeSeriesUpdater<String, FDate> newUpdater(final ATimeSeriesDB<String, FDate> table,
            final List<FDate> values, final boolean redoLastFile) {
        return new ATimeSeriesUpdater<String, FDate>(KEY, table) {

            @Override
            protected ICloseableIterable<? extends FDate> getSource(final FDate updateFrom) {
                return WrapperCloseableIterable.maybeWrap(values);
            }

            @Override
            protected boolean shouldRedoLastFile() {
                return redoLastFile;
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

    private static List<FDate> newValues(final int fromDay, final int toDay) {
        final List<FDate> values = new ArrayList<>();
        for (int day = fromDay; day <= toDay; day++) {
            values.add(FDateBuilder.newDate(2026, 1, day));
        }
        return values;
    }

    private static List<MemoryFileSummary> readPublishedSummaries(
            final TimeSeriesLookupStorageCache<String, FDate> lookup) {
        final List<MemoryFileSummary> summaries = new ArrayList<>();
        try (ICloseableIterator<MemoryFileSummary> it = lookup.getMemoryFileLookupTable().range()) {
            Lists.toListWithoutHasNext(it, summaries);
        }
        return summaries;
    }

    private List<FDate> readValues(final ATimeSeriesDB<String, FDate> table) {
        final List<FDate> values = new ArrayList<>();
        try (ICloseableIterator<FDate> it = table.rangeValues(KEY, FDates.MIN_DATE, FDates.MAX_DATE).iterator()) {
            Lists.toListWithoutHasNext(it, values);
        }
        return values;
    }
}
