package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.retry.NonBlockingRetryLaterRuntimeException;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
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
import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLock;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FDates;

/**
 * The demand-side adoption semantics that the cold bootstrap runs proved load-bearing:
 * <ul>
 * <li>a SELF-SERVED re-demand (the very task holding a key's pin demands its own key again mid-pass) proceeds on the
 * CURRENT published state without adoption - only this case may report a null updatedTo, and the in-flight pass
 * completes and publishes normally;</li>
 * <li>a foreign pin holder with an empty store keeps the loud 'Adoption found empty store' NonBlockingRetryLater
 * refusal byte for byte (zero regression) and publishes nothing itself;</li>
 * <li>a published store always wins: a demand that breaks its pin wait while the store holds a complete generation
 * adopts that published state, and an in-progress attempt of the same key is never read; once that attempt is aborted
 * it publishes what it FINISHED and the reclaim takes only the batch it never finished.</li>
 * </ul>
 */
@NotThreadSafe
public class TimeSeriesSelfServedDemandTest extends ATest {

    private static final int BATCH_FLUSH_INTERVAL = 4;
    private static final String CONSUMER_KEY = "selfServedConsumer";
    private static final String PRODUCER_KEY = "selfServedProducer";

    private File baseDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY,
                TimeSeriesSelfServedDemandTest.class.getSimpleName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Files.deleteQuietly(baseDirectory);
    }

    /**
     * The self-served skip: the task serving key K re-demands K mid-pass while the published store is EMPTY. The
     * demand must not go through adoption-or-refuse: it proceeds (no throw) with the current published state
     * (updatedTo == null only for this self-served case, nothing of the in-flight pass pulled in) and the owning pass
     * completes and publishes all its values normally afterwards.
     */
    @Test
    public void testSelfServedReentrantDemandOnEmptyStoreProceedsAndPassPublishes() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("selfServed");
        final TimeSeriesLookupStorageCache<String, FDate> producerLookup = table.getLookupTableCache(PRODUCER_KEY);
        final List<FDate> producerValues = newValues(1, 12);

        final DemandObservations observations = new DemandObservations();

        //the producer pass: exactly at source exhaustion (two batches flushed, the rest landing after the demand
        //returns) it nests the consumer pass whose innermost demand re-demands the producer key on this very thread
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
                            throw FastNoSuchElementException.getInstance("producer source exhausted");
                        }
                        return producerValues.get(index++);
                    }

                    @Override
                    public void close() {}

                    private void afterExhausted() {
                        try {
                            runNestedConsumerDemand(table, observations);
                        } catch (final IncompleteUpdateRetryableException e) {
                            throw new RuntimeException(e);
                        }
                        //NO abort this time: the producer pass continues and lands everything (a self-served demand
                        //is a scheduling decision, this pass's own output is what will land)
                    }
                };
            }
        };

        final ATimeSeriesUpdater<String, FDate> producer = newUpdater(table, PRODUCER_KEY, producerSource, false);
        producer.update().close();

        //the innermost self-served demand PROCEEDED: a result without a throw...
        final TimeSeriesUpdaterResult demandResult = observations.demandResult.get();
        Assertions.checkNotNull(demandResult, "the self-served demand must have produced a result");
        //...with a null updatedTo (the ONLY case where one may be null: the published store was still empty, and the
        //in-flight producer pass is this very demand tree's own attempt)
        Assertions.checkNull(demandResult.getUpdatedTo(),
                "the self-served demand on an empty store proceeds with a null updatedTo");
        //the demanding read saw the current published (empty) state only, and nothing was pulled into the store
        Assertions.checkNotNull(observations.demandViewValues.get());
        Assertions.checkTrue(observations.demandViewValues.get().isEmpty(),
                "the self-served demand must see the published state only, got: "
                        + observations.demandViewValues.get());
        Assertions.checkTrue(observations.publishedSummariesAtAdoption.get().isEmpty(),
                "a self-served demand must not make the in-flight pass observable: "
                        + observations.publishedSummariesAtAdoption.get());
        //and the consumer attempt completed with its own finished marker
        final TimeSeriesUpdaterProgress consumerMarker = TimeSeriesUpdaterProgress
                .readUpdateProgress(observations.consumerUpdater.get().getUpdateFinishedFile());
        Assertions.checkNotNull(consumerMarker, "the consumer attempt must have left its finished marker");

        //the producer pass completed and published normally: all values are there
        Assertions.assertThat(readValues(table, PRODUCER_KEY)).isEqualTo(new ArrayList<>(producerValues));
        Assertions.checkFalse(readPublishedSummaries(producerLookup).isEmpty(),
                "the producer pass must have published its files");
    }

    /**
     * The 'Adoption found empty store' refusal must keep firing, unchanged, for a demand that breaks its pin wait on
     * a FINISHED marker of a foreign holder while the published store is empty: no self-served shortcut, no
     * unverifiable adoption - plain retryable without touching the store.
     */
    @Test
    public void testForeignEmptyAdoptionRefusalStillFiresUnchanged() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("foreignRefusal");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(PRODUCER_KEY);

        //a foreign holder pins the update lock and leaves a finished marker (the shape of an update that finished a
        //wipe pass whose values never landed): the store's published index stays empty
        final File progressFile = lookup.getUpdateProgressFile();
        try (HeartbeatFileChannelLock foreignPin = new HeartbeatFileChannelLock(
                new File(progressFile.getAbsolutePath() + ".lock"))) {
            Assertions.checkTrue(foreignPin.tryLock(), "the foreign pin fixture must acquire the update lock");
            TimeSeriesUpdaterProgress.writeUpdateProgress(lookup.getUpdateFinishedFile(), FDate.now(), 0, 0, null,
                    null);

            final ATimeSeriesUpdater<String, FDate> demand = newUpdater(table, PRODUCER_KEY,
                    WrapperCloseableIterable.maybeWrap(new ArrayList<FDate>()), false);
            boolean refused = false;
            try {
                demand.update();
            } catch (final NonBlockingRetryLaterRuntimeException e) {
                refused = true;
                Assertions.assertThat(e.getMessage()).contains("Adoption found empty store");
            }
            Assertions.checkTrue(refused,
                    "a non-self-served demand on an empty store must keep the loud empty-adoption refusal");
        }
        Assertions.checkTrue(readPublishedSummaries(lookup).isEmpty(),
                "the refusal must not have published anything");
    }

    /**
     * A published store always wins: with a complete generation on disk, the demand adopts the published state (never
     * what an in-progress attempt of the same key has staged so far). The attempt that is aborted afterwards keeps what
     * it FINISHED and the reclaim takes only its unfinished batch - the adopted generation's bytes stay untouched, and
     * a demand that breaks its pin wait mid-pass still reads only what was published at that moment.
     */
    @Test
    public void testPublishedGenerationIsPreferredOverInProgressAttempt() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("publishedWins");
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

        //the demand adopted the PUBLISHED state (day 4), not the in-progress attempt's staged values
        final TimeSeriesUpdaterResult demandResult = observations.demandResult.get();
        Assertions.checkNotNull(demandResult);
        Assertions.checkEquals(newValues(1, 4).get(3), demandResult.getUpdatedTo(),
                "the published store must win over the in-progress attempt");
        Assertions.assertThat(observations.demandViewValues.get()).isEqualTo(newValues(1, 4));

        //the consumer attempt completed on that published state
        final TimeSeriesUpdaterProgress consumerMarker = TimeSeriesUpdaterProgress
                .readUpdateProgress(observations.consumerUpdater.get().getUpdateFinishedFile());
        Assertions.checkNotNull(consumerMarker, "the consumer attempt must have left its finished marker");

        //What the aborted second attempt FINISHED is store content once its transaction closes: the nested demand saw
        //the published days 1-4 while the attempt was in progress, and after the abort the store reads the finished file
        //the attempt landed as well (days 9-12 were still in flight when the source died, so that batch is reclaimed).
        //The generation the demand adopted is untouched.
        Assertions.assertThat(readValues(table, PRODUCER_KEY)).isEqualTo(newValues(1, 8));
        final List<MemoryFileSummary> published = readPublishedSummaries(producerLookup);
        Assertions.checkFalse(published.isEmpty());
        final String abortedAttemptId = producer.getEpochId();
        Assertions.checkNotNull(abortedAttemptId, "the attempt must have minted its data file id");
        final List<String> publishedNames = new ArrayList<>();
        for (final MemoryFileSummary summary : published) {
            publishedNames.add(new File(summary.getMemoryResourceUri()).getName());
        }
        for (final File own : findAttemptDataFiles(producerLookup, abortedAttemptId)) {
            Assertions.checkTrue(publishedNames.contains(own.getName()),
                    "the aborted attempt left an unreferenced data file behind: " + own);
            Assertions.checkFalse(MemoryFiles.isIncompleteMemoryFile(own.getName()),
                    "an aborted attempt must not leave '_incomplete_' litter behind: " + own);
        }
        //and every data file the surviving published generation references is still there with its bytes
        for (final MemoryFileSummary summary : published) {
            final File dataFile = new File(summary.getMemoryResourceUri());
            Assertions.checkTrue(dataFile.exists(), "the reclaim must not remove a referenced data file: " + dataFile);
        }
    }

    @NotThreadSafe
    private static final class DemandObservations {
        private final AtomicReference<TimeSeriesUpdaterResult> demandResult = new AtomicReference<>();
        private final AtomicReference<List<FDate>> demandViewValues = new AtomicReference<>();
        private final AtomicReference<List<MemoryFileSummary>> publishedSummariesAtAdoption = new AtomicReference<>();
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
                            //immediate demand-side reads: only a published state is ever visible to a demand
                            observations.demandViewValues.set(readValues(table, PRODUCER_KEY));
                            observations.publishedSummariesAtAdoption.set(
                                    readPublishedSummaries(table.getLookupTableCache(PRODUCER_KEY)));
                        } finally {
                            demandResult.close();
                        }
                        adoptedValues = new ArrayList<>(observations.demandViewValues
                                .get().subList(0, Integers.min(BATCH_FLUSH_INTERVAL, observations.demandViewValues
                                        .get().size())));
                    }
                };
            }
        };
        final ATimeSeriesUpdater<String, FDate> consumer = newUpdater(table, CONSUMER_KEY, consumerSource, false);
        observations.consumerUpdater.set(consumer);
        final TimeSeriesUpdaterResult consumerResult = consumer.update();
        consumerResult.close();
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

    private static TimeSeriesUpdaterResult demandUpdate(final ATimeSeriesUpdater<String, FDate> demand) {
        try {
            return demand.update();
        } catch (final IncompleteUpdateRetryableException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<File> findAttemptDataFiles(final TimeSeriesLookupStorageCache<String, FDate> lookup,
            final String attemptId) {
        final File dataDirectory = lookup.getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        final File[] files = dataDirectory
                .listFiles((dir, name) -> name.startsWith(MemoryFiles.EPOCH_LOCAL_DATA_FILE_PREFIX + attemptId));
        final List<File> found = new ArrayList<>();
        if (files != null) {
            for (final File file : files) {
                found.add(file);
            }
        }
        return found;
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
