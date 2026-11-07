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
import de.invesdwin.util.math.Longs;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FDates;

/**
 * The standalone '_incomplete_' file is deliberately the provisional shard: each pass redoes it so that it either FILLS
 * up to a full batch (and becomes a final immutable shard) or gets its LAST BAR updated. Full shards are never redone -
 * that boundary is the trust model, and these tests keep it.
 * <p>
 * The normal redo path is checkless by design: the pass starts in front of the last bar's first constituent, replays
 * the same input and regenerates that bar, so replacing it blind is correct. Only one branch is new - the ANOMALY, the
 * redo window not being reproducible because the source no longer re-serves the required constituents - and it is
 * decided by the validated-final watermark (the {@code FINAL_END_TIME} property of the key), raised by two
 * attestations: a successor observed in the chain, and a pass that consumed its source inside a requested range.
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
     * Normal path, unchanged: the source re-serves the dropped position (the last-bar update) and brings more values
     * that FILL the provisional shard to a full one. Its answer wins even though the previous pass sealed that value -
     * sealing protects against loss, never against an update - and each value is written exactly once.
     */
    @Test
    public void testNormalReplayReplaceOfTheTailIsUnchangedAndFillsTheShard() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("redoTailServed");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final List<FDate> firstPass = newValues(1, BATCH_FLUSH_INTERVAL + 2);
        newUpdater(table, firstPass, newDate(BATCH_FLUSH_INTERVAL + 5)).update().close();
        //the pass consumed its source inside a requested range: its last value is sealed
        Assertions.assertThat(lookup.getFinalEndTime()).as("watermark after the sealed pass")
                .isEqualTo(firstPass.get(firstPass.size() - 1));
        final long highWaterFirst = chainHighWater(lookup);

        //the redo's dropped tail comes back from the source, plus new values behind it that fill a full shard
        final List<FDate> secondPass = newValues(1, BATCH_FLUSH_INTERVAL * 3 + 2);
        newUpdater(table, secondPass, newDate(BATCH_FLUSH_INTERVAL * 3 + 5)).update().close();

        Assertions.assertThat(readValues(table, KEY)).as("content must be the source answer, each value exactly once")
                .isEqualTo(new ArrayList<>(secondPass));
        Assertions.assertThat(chainHighWater(lookup)).as("byte high-water must grow with the extra values")
                .isGreaterThan(highWaterFirst);
        final List<MemoryFileSummary> published = readPublishedSummaries(lookup);
        int fullShards = 0;
        MemoryFileSummary provisionalTail = null;
        for (final MemoryFileSummary summary : published) {
            if (MemoryFiles.isIncompleteMemoryFile(summary.getMemoryResourceUri())) {
                provisionalTail = summary;
            } else {
                fullShards++;
            }
        }
        Assertions.assertThat(fullShards).as("the filled region produced full shards: " + published)
                .isGreaterThanOrEqualTo(3);
        Assertions.assertThat(provisionalTail).as("a new provisional tail holds the newest values: " + published)
                .isNotNull();
        Assertions.assertThat(provisionalTail.getValueCount())
                .as("only the values behind the last full batch stay provisional: " + published).isEqualTo(2);
        Assertions.assertThat(lookup.getFinalEndTime()).as("watermark follows the newly written values")
                .isEqualTo(secondPass.get(secondPass.size() - 1));
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(), "the store must stay consistent: " + published);
    }

    /**
     * The pair's shape: closed history, the pass works a requested range and its source runs dry inside it, so the tail
     * is sealed; a later attempt whose supply no longer re-serves that position (the anomaly) must not lose it. The
     * published chain stays identical across alternating short / complete attempts - a function of the previous
     * generation plus the attestation, not of which attempt ran.
     */
    @Test
    public void testAnomalyWithSealedTailRepublishesItStably() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("redoTailSealed");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final List<FDate> full = newValues(1, BATCH_FLUSH_INTERVAL + 2);
        final List<FDate> shortSupply = full.subList(0, full.size() - 1);
        final FDate requestedUpdateTo = newDate(BATCH_FLUSH_INTERVAL + 5);
        newUpdater(table, full, requestedUpdateTo).update().close();
        final long highWaterFirst = chainHighWater(lookup);
        Assertions.assertThat(readValues(table, KEY)).as("pass 1 content").isEqualTo(new ArrayList<>(full));

        for (int attempt = 1; attempt <= 3; attempt++) {
            final List<FDate> supply = attempt % 2 == 1 ? shortSupply : full;
            newUpdater(table, supply, requestedUpdateTo).update().close();
            Assertions.assertThat(readValues(table, KEY)).as("content after anomaly attempt " + attempt)
                    .isEqualTo(new ArrayList<>(full));
            Assertions.assertThat(chainHighWater(lookup)).as("byte high-water after anomaly attempt " + attempt)
                    .isEqualTo(highWaterFirst);
            Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                    "the store must stay consistent: " + readPublishedSummaries(lookup));
        }
    }

    /**
     * The anomaly with the tail ABOVE the watermark (no attestation: no requested range end, no successor behind it):
     * the shrink is legal - that is the provisional bar being left out for the next pass to recompute - and the next
     * pass whose supply reaches the position fills it back in.
     */
    @Test
    public void testAnomalyWithProvisionalTailShrinksLegallyAndRecomputes() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("redoTailProvisional");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final List<FDate> full = newValues(1, BATCH_FLUSH_INTERVAL + 2);
        newUpdater(table, full, null).update().close();
        //only the successor attestation applies: the value before the trailing provisional one is sealed
        Assertions.assertThat(lookup.getFinalEndTime()).as("watermark covers the successor-sealed shard only")
                .isEqualTo(newDate(BATCH_FLUSH_INTERVAL));

        //pass 2: the supply does not re-serve the tail, which sits above the watermark -> shrink legal
        newUpdater(table, full.subList(0, full.size() - 1), null).update().close();
        Assertions.assertThat(readValues(table, KEY)).as("a provisional tail above the watermark may still shrink")
                .isEqualTo(new ArrayList<>(full.subList(0, full.size() - 1)));

        //pass 3: the supply reaches the position again -> the provisional shard recomputes and fills
        newUpdater(table, full, null).update().close();
        Assertions.assertThat(readValues(table, KEY)).as("the provisional region must recompute on the next pass")
                .isEqualTo(new ArrayList<>(full));
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "the store must stay consistent: " + readPublishedSummaries(lookup));
    }

    /**
     * The successor attestation advances the watermark mid-chain: a shard that gains a later shard seals the value that
     * preceded it, and the next arrival seals the one before that. Nothing here depends on periods or on a requested
     * range - a later distinct bar at or after a value's end is the whole attestation.
     */
    @Test
    public void testSuccessorArrivalAdvancesTheWatermarkMidChain() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("redoTailSuccessor");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        newUpdater(table, newValues(1, BATCH_FLUSH_INTERVAL), null).update().close();
        Assertions.assertThat(lookup.getFinalEndTime()).as("a single shard with no successor seals nothing")
                .isNull();

        newUpdater(table, newValues(1, BATCH_FLUSH_INTERVAL * 2), null).update().close();
        Assertions.assertThat(lookup.getFinalEndTime()).as("the second shard seals the value that precedes it")
                .isEqualTo(newDate(BATCH_FLUSH_INTERVAL));

        newUpdater(table, newValues(1, BATCH_FLUSH_INTERVAL * 3), null).update().close();
        Assertions.assertThat(lookup.getFinalEndTime()).as("the third shard seals the value that precedes it")
                .isEqualTo(newDate(BATCH_FLUSH_INTERVAL * 2));
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "the store must stay consistent: " + readPublishedSummaries(lookup));
    }

    /**
     * The anomaly whose supply skipped the sealed position and answered LATER positions must keep the re-published
     * value in time order - in front of those answers, with the chain strictly ascending.
     */
    @Test
    public void testAnomalyWithSealedTailKeepsItInTimeOrderBeforeLaterAnswers() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("redoTailGap");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final FDate requestedUpdateTo = newDate(BATCH_FLUSH_INTERVAL + 9);
        final List<FDate> firstPass = newValues(1, BATCH_FLUSH_INTERVAL + 2);
        newUpdater(table, firstPass, requestedUpdateTo).update().close();

        //the supply stops short of the sealed tail and then answers later positions
        final List<FDate> secondPass = new ArrayList<>(firstPass.subList(0, firstPass.size() - 1));
        secondPass.addAll(newValues(BATCH_FLUSH_INTERVAL + 5, BATCH_FLUSH_INTERVAL + 6));
        newUpdater(table, secondPass, requestedUpdateTo).update().close();

        final List<FDate> expected = new ArrayList<>(firstPass);
        expected.addAll(newValues(BATCH_FLUSH_INTERVAL + 5, BATCH_FLUSH_INTERVAL + 6));
        final List<FDate> content = readValues(table, KEY);
        Assertions.assertThat(content).as("the re-published tail must sit in time order before the later answers")
                .isEqualTo(expected);
        for (int i = 1; i < content.size(); i++) {
            Assertions.checkTrue(content.get(i - 1).isBeforeNotNullSafe(content.get(i)),
                    "the chain must stay strictly ascending: " + content);
        }
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "the store must stay consistent: " + readPublishedSummaries(lookup));
    }

    private static long chainHighWater(final TimeSeriesLookupStorageCache<String, FDate> lookup) {
        long highest = 0L;
        for (final MemoryFileSummary summary : readPublishedSummaries(lookup)) {
            highest = Longs.max(highest,
                    summary.getPrecedingMemoryOffset() + summary.getMemoryOffset() + summary.getMemoryLength());
        }
        return highest;
    }

    private static FDate newDate(final int day) {
        return FDateBuilder.newDate(2020, 1, 1).addDays(day);
    }

    private static List<FDate> newValues(final int fromDay, final int toDay) {
        final List<FDate> values = new ArrayList<>();
        for (int day = fromDay; day <= toDay; day++) {
            values.add(newDate(day));
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
            final List<FDate> values, final FDate requestedUpdateTo) {
        return new ATimeSeriesUpdater<String, FDate>(KEY, table) {

            @Override
            protected ICloseableIterable<? extends FDate> getSource(final FDate updateFrom) {
                return WrapperCloseableIterable.maybeWrap(values);
            }

            @Override
            protected FDate getRequestedUpdateTo() {
                return requestedUpdateTo;
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
