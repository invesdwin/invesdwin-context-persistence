package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdateProgress;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.lang.string.Charsets;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FDates;

/**
 * Epoch design M2 (in-epoch read visibility): a demanding pass nested inside a demand tree that owns a live update
 * attempt (epoch) must see that attempt's staged index snapshot where it would previously hit the
 * 'Adoption found empty store' refusal, and the published generation must still win wherever one exists. The staged
 * snapshot is only ever read from the atomic 'index.snapshot' file (temp+rename per flush), never from the
 * partially-appended index of the in-progress transaction, and it never becomes visible outside the adopting thread's
 * demand (see {@link TimeSeriesLookupStorageCache#adoptEpochSnapshot(String, List)}).
 */
@NotThreadSafe
public class TimeSeriesEpochSnapshotVisibilityTest extends ATest {

    private static final int BATCH_FLUSH_INTERVAL = 4;
    private static final String CONSUMER_KEY = "epochVisibilityConsumer";
    private static final String PRODUCER_KEY = "epochVisibilityProducer";

    private File baseDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY,
                TimeSeriesEpochSnapshotVisibilityTest.class.getSimpleName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Files.deleteQuietly(baseDirectory);
    }

    /**
     * The producer key is never published: its attempt stages two full batches (8 values) and then, exactly at the
     * third batch boundary, nests a demand on the consumer key, whose source demands the producer key again. That
     * innermost demand used to die with 'Adoption found empty store'; with M2 it adopts the producer's staged epoch
     * snapshot: updatedTo is the highest firstValueEndTime among the staged summaries and the immediate demand-side
     * reads see exactly the staged values (count matches the flush point). After the demand the producer attempt is
     * aborted (its values are never committed), so the visibility observed here can only have come from the staged
     * epoch snapshot - and the consumer's progress marker carries the consumed '&lt;key&gt;#&lt;epochId&gt;' ledger.
     */
    @Test
    public void testNestedDemandAdoptsStagedEpochSnapshotInsteadOfEmptyStore() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("epochVisibility");
        final TimeSeriesLookupStorageCache<String, FDate> producerLookup = table.getLookupTableCache(PRODUCER_KEY);
        final List<FDate> producerValues = newValues(1, 12);

        final DemandObservations observations = new DemandObservations();

        //the producer attempt: after its second full batch has landed (two staged summaries), the completeness probe
        //for the third batch triggers the nested consumer demand and then aborts the attempt itself
        final ICloseableIterable<FDate> producerSource = new ICloseableIterable<FDate>() {
            @Override
            public ICloseableIterator<FDate> iterator() {
                return new ICloseableIterator<FDate>() {

                    private int index = 0;

                    @Override
                    public boolean hasNext() {
                        if (index < producerValues.size()) {
                            return true;
                        }
                        afterExhausted();
                        return false;
                    }

                    @Override
                    public FDate next() {
                        if (index >= producerValues.size()) {
                            afterExhausted();
                            throw FastNoSuchElementException
                                    .getInstance("producer source exhausted");
                        }
                        return producerValues.get(index++);
                    }

                    @Override
                    public void close() {}

                    private void afterExhausted() {
                        //the demand runs exactly once, right after the third full batch has been consumed; it nests
                        //the whole consumer pass (and thereby the innermost demand on the producer key)
                        try {
                            runNestedConsumerDemand(table, observations);
                        } catch (final IncompleteUpdateRetryableException e) {
                            throw new RuntimeException(e);
                        }
                        //abort the producer attempt right after the demand: its store must stay unpublished
                        throw new IllegalStateException("abort producer attempt after nested demand");
                    }
                };
            }
        };

        final ATimeSeriesUpdater<String, FDate> producer = newUpdater(table, PRODUCER_KEY, producerSource, false);
        boolean aborted = false;
        try {
            producer.update().close();
        } catch (final Exception e) {
            //IncompleteUpdateRetryableException wrapping the abort: expected, the attempt discards everything
            aborted = true;
        }
        Assertions.checkTrue(aborted, "the aborted producer attempt must not report success");

        final String producerEpochId = producer.getEpochId();
        Assertions.checkTrue(producerEpochId != null, "the producer attempt must have minted an epoch id");

        //the innermost demand adopted the staged snapshot instead of throwing
        final TimeSeriesUpdaterResult demandResult = observations.demandResult.get();
        Assertions.checkNotNull(demandResult, "the nested demand must have produced an adoption result");
        //two full batches staged at the flush point: the highest firstValueEndTime is the start of the second (day 5)
        Assertions.checkEquals(producerValues.get(BATCH_FLUSH_INTERVAL), demandResult.getUpdatedTo(),
                "updatedTo must be the highest staged firstValueEndTime");

        //the snapshot seen at adoption time: exactly the two flushed summaries
        Assertions.checkNotNull(observations.stagedSnapshot.get(), "a staged epoch snapshot must exist");
        Assertions.checkEquals(2, observations.stagedSnapshot.get().size(),
                "two batches were flushed at the adoption point");

        //the immediate demand-side reads saw the staged values (count matches the flush point)
        Assertions.checkNotNull(observations.demandViewValues.get());
        Assertions.assertThat(observations.demandViewValues.get())
                .isEqualTo(new ArrayList<>(producerValues.subList(0, 2 * BATCH_FLUSH_INTERVAL)));

        //the published generation stayed empty at adoption (in-epoch visibility, nothing was published by it)
        Assertions.checkNotNull(observations.publishedSummariesAtAdoption.get());
        Assertions.checkTrue(observations.publishedSummariesAtAdoption.get().isEmpty(),
                "the adopted state must not have become a published generation: "
                        + observations.publishedSummariesAtAdoption.get());

        //the aborted attempt published nothing: the store's published index is still empty on disk
        Assertions.checkTrue(readPublishedSummaries(producerLookup).isEmpty(),
                "the aborted producer attempt must not have published anything");

        //the epoch snapshot lives in the non-numeric staging directory, outside the version's data directory
        final File stagingDir = TimeSeriesUpdateTransaction.getEpochStagingDirectory(producerLookup, producerEpochId);
        Assertions.checkEquals("epoch-staging", stagingDir.getParentFile().getName());
        final File snapshotFile = new File(stagingDir,
                TimeSeriesUpdateTransaction.EPOCH_INDEX_SNAPSHOT_FILE_NAME);
        Assertions.checkTrue(snapshotFile.exists(), "the epoch index snapshot file must exist: " + snapshotFile);
        final List<MemoryFileSummary> stagedFinal = TimeSeriesUpdateTransaction.readEpochIndexSnapshot(producerLookup,
                producerEpochId);
        Assertions.checkNotNull(stagedFinal);
        Assertions.checkEquals(2, stagedFinal.size(), "two flushes landed snapshots");

        //the consumer attempt published its own store from the adopted values and recorded the consumed epoch ledger
        final List<FDate> publishedConsumerValues = readValues(table, CONSUMER_KEY);
        Assertions.assertThat(publishedConsumerValues)
                .isEqualTo(new ArrayList<>(producerValues.subList(0, BATCH_FLUSH_INTERVAL)));
        final TimeSeriesUpdaterProgress consumerMarker = TimeSeriesUpdaterProgress
                .readUpdateProgress(observations.consumerUpdater.get().getUpdateFinishedFile());
        Assertions.checkNotNull(consumerMarker, "the consumer attempt must have left its finished marker");
        Assertions.checkEquals(observations.consumerUpdater.get().getEpochId(), consumerMarker.getEpochId());
        Assertions.checkEquals(Arrays.asList(PRODUCER_KEY + "#" + producerEpochId),
                consumerMarker.getConsumedEpochEntries());
    }

    @NotThreadSafe
    private static final class DemandObservations {
        private final AtomicReference<TimeSeriesUpdaterResult> demandResult = new AtomicReference<>();
        private final AtomicReference<List<FDate>> demandViewValues = new AtomicReference<>();
        private final AtomicReference<List<MemoryFileSummary>> publishedSummariesAtAdoption = new AtomicReference<>();
        private final AtomicReference<List<MemoryFileSummary>> stagedSnapshot = new AtomicReference<>();
        private final AtomicReference<ATimeSeriesUpdater<String, FDate>> consumerUpdater = new AtomicReference<>();
    }

    private void runNestedConsumerDemand(final ATimeSeriesDB<String, FDate> table,
            final DemandObservations observations) throws IncompleteUpdateRetryableException {
        //the consumer's source: the very first pull demands the producer key - that producer attempt is still
        //in-flight with two flushed batches and an empty published store
        final ICloseableIterable<FDate> consumerSource = new ICloseableIterable<FDate>() {
            @Override
            public ICloseableIterator<FDate> iterator() {
                return new ICloseableIterator<FDate>() {

                    private List<FDate> adoptedValues;
                    private int index = 0;

                    @Override
                    public boolean hasNext() {
                        ensureDemandRun();
                        return index < adoptedValues.size();
                    }

                    @Override
                    public FDate next() {
                        ensureDemandRun();
                        if (index >= adoptedValues.size()) {
                            throw FastNoSuchElementException.getInstance("source exhausted");
                        }
                        return adoptedValues.get(index++);
                    }

                    @Override
                    public void close() {}

                    private void ensureDemandRun() {
                        if (adoptedValues != null) {
                            return;
                        }
                        //a source that is never reached: the demand updater takes the remote-tracking branch
                        final ATimeSeriesUpdater<String, FDate> demand = newUpdater(table, PRODUCER_KEY,
                                WrapperCloseableIterable.maybeWrap(new ArrayList<FDate>()), false);
                        final TimeSeriesUpdaterResult demandResult = demandUpdate(demand);
                        observations.demandResult.set(demandResult);
                        try {
                            //immediate demand-side reads: the adopted epoch view serves the staged values
                            observations.demandViewValues.set(readValues(table, PRODUCER_KEY));
                            observations.publishedSummariesAtAdoption.set(
                                    readPublishedSummaries(table.getLookupTableCache(PRODUCER_KEY)));
                            observations.stagedSnapshot.set(TimeSeriesUpdateTransaction.readEpochIndexSnapshot(
                                    table.getLookupTableCache(PRODUCER_KEY), readMarkerEpochId(table)));
                        } finally {
                            demandResult.close();
                        }
                        adoptedValues = new ArrayList<>(
                                observations.demandViewValues.get().subList(0, BATCH_FLUSH_INTERVAL));
                    }
                };
            }
        };
        final ATimeSeriesUpdater<String, FDate> consumer = newUpdater(table, CONSUMER_KEY, consumerSource, false);
        observations.consumerUpdater.set(consumer);
        final TimeSeriesUpdaterResult consumerResult = consumer.update();
        consumerResult.close();
    }

    private static TimeSeriesUpdaterResult demandUpdate(final ATimeSeriesUpdater<String, FDate> demand) {
        try {
            return demand.update();
        } catch (final IncompleteUpdateRetryableException e) {
            throw new RuntimeException(e);
        }
    }

    private String readMarkerEpochId(final ATimeSeriesDB<String, FDate> table) {
        final TimeSeriesUpdaterProgress marker = TimeSeriesUpdaterProgress
                .readUpdateProgress(table.getLookupTableCache(PRODUCER_KEY).getUpdateProgressFile());
        return marker != null ? marker.getEpochId() : null;
    }

    /**
     * A published store always wins: with a complete generation on disk, an in-progress epoch of the same key is not
     * consulted for adoption, the adoption returns the published max (not the staged max) and the demand-side reads
     * keep seeing the published values only; nothing lands in the consumed ledger.
     */
    @Test
    public void testPublishedGenerationIsPreferredOverLiveEpochSnapshot() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("epochVisibilityLegacy");
        final TimeSeriesLookupStorageCache<String, FDate> producerLookup = table.getLookupTableCache(PRODUCER_KEY);

        //publish days 1-4 normally
        newUpdater(table, PRODUCER_KEY, WrapperCloseableIterable.maybeWrap(newValues(1, 4)), false).update().close();
        Assertions.assertThat(readValues(table, PRODUCER_KEY)).isEqualTo(newValues(1, 4));

        final DemandObservations observations = new DemandObservations();

        //a second producer attempt stages more values but is aborted after the nested demand
        final ICloseableIterable<FDate> producerSource = new ICloseableIterable<FDate>() {
            @Override
            public ICloseableIterator<FDate> iterator() {
                return new ICloseableIterator<FDate>() {

                    private final List<FDate> values = newValues(1, 12);
                    private int index = 0;

                    @Override
                    public boolean hasNext() {
                        if (index < values.size()) {
                            return true;
                        }
                        afterExhausted();
                        return false;
                    }

                    @Override
                    public FDate next() {
                        if (index >= values.size()) {
                            afterExhausted();
                            throw FastNoSuchElementException.getInstance("source exhausted");
                        }
                        return values.get(index++);
                    }

                    @Override
                    public void close() {}

                    private void afterExhausted() {
                        try {
                            runConsumerPublishedAdoptionDemand(table, observations);
                        } catch (final IncompleteUpdateRetryableException e) {
                            throw new RuntimeException(e);
                        }
                        throw new IllegalStateException("abort producer attempt after nested demand");
                    }
                };
            }
        };
        final ATimeSeriesUpdater<String, FDate> producer = newUpdater(table, PRODUCER_KEY, producerSource, false);
        boolean aborted = false;
        try {
            producer.update().close();
        } catch (final Exception e) {
            aborted = true;
        }
        Assertions.checkTrue(aborted, "the aborted producer attempt must not report success");

        //the demand adopted the PUBLISHED state (day 4), not an epoch snapshot (which would report at least day 5)
        final TimeSeriesUpdaterResult demandResult = observations.demandResult.get();
        Assertions.checkNotNull(demandResult);
        Assertions.checkEquals(newValues(1, 4).get(3), demandResult.getUpdatedTo(),
                "the published store must win over the live epoch snapshot");
        Assertions.assertThat(observations.demandViewValues.get()).isEqualTo(newValues(1, 4));

        //and the consumer attempt recorded no consumed-epoch ledger (nothing epoch-adopted)
        final TimeSeriesUpdaterProgress consumerMarker = TimeSeriesUpdaterProgress
                .readUpdateProgress(observations.consumerUpdater.get().getUpdateFinishedFile());
        Assertions.checkNotNull(consumerMarker);
        Assertions.checkTrue(consumerMarker.getEpochId() != null,
                "with the epoch flag on the consumer attempt still names its own epoch");
        Assertions.checkTrue(consumerMarker.getConsumedEpochEntries().isEmpty(),
                "published-state adoption must not record an epoch consumption: "
                        + consumerMarker.getConsumedEpochEntries());

        //the aborted second attempt changed nothing observable
        Assertions.assertThat(readValues(table, PRODUCER_KEY)).isEqualTo(newValues(1, 4));
        Assertions.checkFalse(readPublishedSummaries(producerLookup).isEmpty());
    }

    private void runConsumerPublishedAdoptionDemand(final ATimeSeriesDB<String, FDate> table,
            final DemandObservations observations) throws IncompleteUpdateRetryableException {
        final ICloseableIterable<FDate> consumerSource = new ICloseableIterable<FDate>() {
            @Override
            public ICloseableIterator<FDate> iterator() {
                return new ICloseableIterator<FDate>() {

                    private List<FDate> adoptedValues;
                    private int index = 0;

                    @Override
                    public boolean hasNext() {
                        ensureDemandRun();
                        return index < adoptedValues.size();
                    }

                    @Override
                    public FDate next() {
                        ensureDemandRun();
                        if (index >= adoptedValues.size()) {
                            throw FastNoSuchElementException.getInstance("source exhausted");
                        }
                        return adoptedValues.get(index++);
                    }

                    @Override
                    public void close() {}

                    private void ensureDemandRun() {
                        if (adoptedValues != null) {
                            return;
                        }
                        final ATimeSeriesUpdater<String, FDate> demand = newUpdater(table, PRODUCER_KEY,
                                WrapperCloseableIterable.maybeWrap(new ArrayList<FDate>()), false);
                        final TimeSeriesUpdaterResult demandResult = demandUpdate(demand);
                        observations.demandResult.set(demandResult);
                        adoptedValues = readValues(table, PRODUCER_KEY);
                        observations.demandViewValues.set(adoptedValues);
                    }
                };
            }
        };
        final ATimeSeriesUpdater<String, FDate> consumer = newUpdater(table, CONSUMER_KEY, consumerSource, false);
        observations.consumerUpdater.set(consumer);
        consumer.update().close();
    }

    /**
     * The progress marker format stays readable both ways: old 6-field content parses with the new tolerant parser
     * (no epoch, no ledger) and the new 7-field content (epoch id, optionally with comma-joined consumed entries)
     * round-trips.
     */
    @Test
    public void testUpdateProgressFormatIsTolerantForSixAndSevenFields() throws Exception {
        final FDate updateStart = FDateBuilder.newDate(2026, 10, 31);
        final FDate minTime = FDateBuilder.newDate(2020, 1, 1);
        final FDate maxTime = FDateBuilder.newDate(2020, 12, 31);

        final File legacyFile = new File(baseDirectory, "legacy.progress");
        TimeSeriesUpdaterProgress.writeUpdateProgress(legacyFile, updateStart, 3, 42, minTime, maxTime);
        final String legacyContent = Files.readFileToString(legacyFile, Charsets.defaultCharset());
        Assertions.checkEquals(6, Strings.splitPreserveAllTokens(legacyContent, ";").length,
                "legacy content must keep 6 fields: " + legacyContent);
        final TimeSeriesUpdaterProgress legacy = TimeSeriesUpdaterProgress.readUpdateProgress(legacyFile);
        Assertions.checkNotNull(legacy, "the new parser must accept old 6-field content");
        Assertions.checkEquals(updateStart, legacy.getUpdateStart());
        Assertions.checkEquals(3, legacy.getFlushIndex());
        Assertions.checkEquals(42, legacy.getValueCount());
        Assertions.checkEquals(minTime, legacy.getMinTime());
        Assertions.checkEquals(maxTime, legacy.getMaxTime());
        Assertions.checkTrue(legacy.getEpochId() == null, "legacy content carries no epoch");
        Assertions.checkTrue(legacy.getConsumedEpochEntries().isEmpty());

        final File epochFile = new File(baseDirectory, "epoch.progress");
        TimeSeriesUpdaterProgress.writeUpdateProgress(epochFile, updateStart, 5, 77, minTime, maxTime, "1234-abcd",
                null);
        Assertions.checkEquals(7, Strings.splitPreserveAllTokens(
                Files.readFileToString(epochFile, Charsets.defaultCharset()), ";").length, "epoch content must carry a 7th field");
        final TimeSeriesUpdaterProgress epochOnly = TimeSeriesUpdaterProgress.readUpdateProgress(epochFile);
        Assertions.checkNotNull(epochOnly, "the parser must accept 7-field content");
        Assertions.checkEquals("1234-abcd", epochOnly.getEpochId());
        Assertions.checkTrue(epochOnly.getConsumedEpochEntries().isEmpty());

        final File ledgerFile = new File(baseDirectory, "ledger.progress");
        final List<String> consumed = Arrays.asList(CONSUMER_KEY + "#111-aa", PRODUCER_KEY + "#222-bb");
        TimeSeriesUpdaterProgress.writeUpdateProgress(ledgerFile, updateStart, 6, 88, minTime, maxTime, "333-cc",
                consumed);
        final TimeSeriesUpdaterProgress ledger = TimeSeriesUpdaterProgress.readUpdateProgress(ledgerFile);
        Assertions.checkNotNull(ledger);
        Assertions.checkEquals("333-cc", ledger.getEpochId());
        Assertions.checkEquals(consumed, ledger.getConsumedEpochEntries(), "the consumed ledger must round-trip");
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

    private ATimeSeriesUpdater<String, FDate> newUpdater(final ATimeSeriesDB<String, FDate> table, final String key,
            final ICloseableIterable<? extends FDate> source, final boolean parallel) {
        return new ATimeSeriesUpdater<String, FDate>(key, table) {

            @Override
            protected ICloseableIterable<? extends FDate> getSource(final FDate updateFrom) {
                return source;
            }

            @Override
            protected boolean shouldWriteInParallel() {
                return parallel;
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
