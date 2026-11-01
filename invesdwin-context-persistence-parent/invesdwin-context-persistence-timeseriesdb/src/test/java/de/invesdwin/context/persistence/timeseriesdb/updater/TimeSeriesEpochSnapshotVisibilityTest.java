package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.retry.NonBlockingRetryLaterRuntimeException;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.ITimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdateProgress;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdaterInternalMethods;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.SequentialChunkedUpdateProgress;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.concurrent.graph.WaitForGraph;
import de.invesdwin.util.concurrent.lock.file.HeartbeatFileChannelLock;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.string.Charsets;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FDates;

/**
 * Epoch design M2/M3-cp (in-epoch read visibility + self-served skip semantics):
 * <ul>
 * <li>a SELF-SERVED re-demand (the very task holding a key's pin demands its own key again mid-pass) proceeds on
 * the CURRENT published state without adoption and without the 'Adoption found empty store' refusal - only this case
 * may report a null updatedTo, and the in-flight pass completes and publishes normally;</li>
 * <li>an in-tree demand from ANOTHER thread (ancestor-owned pin closing a wait-for cycle) still adopts the live
 * epoch's staged index snapshot where the published store is empty, and records the consumed ledger;</li>
 * <li>a foreign pin holder with an empty store keeps the loud NonBlockingRetryLater 'Adoption found empty store'
 * refusal byte for byte (zero regression);</li>
 * <li>a published store always wins over a live epoch snapshot.</li>
 * </ul>
 * The staged snapshot is only ever read from the atomic 'index.snapshot' file (temp+rename per flush; since M3-cp also
 * written empty at attempt start), never from the partially-appended index of the in-progress transaction, and it
 * never becomes visible outside the adopting thread's demand (see
 * {@link TimeSeriesLookupStorageCache#adoptEpochSnapshot(String, List)}).
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
     * M3-cp self-served skip: the task serving key K re-demands K mid-pass while the published store is EMPTY. The
     * demand must not go through adoption-or-refuse: it proceeds (no throw) with the current published state
     * (updatedTo == null only for this self-served case, no epoch view installed, no consumed ledger) and the owning
     * pass completes and publishes all its values normally afterwards.
     */
    @Test
    public void testSelfServedReentrantDemandOnEmptyStoreProceedsAndPassPublishes() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("epochSelfServed");
        final TimeSeriesLookupStorageCache<String, FDate> producerLookup = table.getLookupTableCache(PRODUCER_KEY);
        final List<FDate> producerValues = newValues(1, 12);

        final DemandObservations observations = new DemandObservations();

        //the producer pass: exactly at source exhaustion (two batches flushed, third landing after the demand returns)
        //it nests the consumer pass whose innermost demand re-demands the producer key on this very thread
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
                        //NO abort this time: the producer pass continues and lands everything (M3-cp semantics: the
                        //self-served demand is a scheduling decision, this pass's own output is what will land)
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
        //no epoch adoption happened: the demanding read saw the current published (empty) state, not the staged snapshot
        Assertions.checkNotNull(observations.demandViewValues.get());
        Assertions.checkTrue(observations.demandViewValues.get().isEmpty(),
                "the self-served demand must see the published state only, got: "
                        + observations.demandViewValues.get());
        //and the consumer attempt recorded no consumed-epoch ledger (nothing epoch-adopted)
        final TimeSeriesUpdaterProgress consumerMarker = TimeSeriesUpdaterProgress
                .readUpdateProgress(observations.consumerUpdater.get().getUpdateFinishedFile());
        Assertions.checkNotNull(consumerMarker);
        Assertions.checkTrue(consumerMarker.getConsumedEpochEntries().isEmpty(),
                "a self-served demand must not record an epoch consumption: "
                        + consumerMarker.getConsumedEpochEntries());

        //the producer pass completed and published normally: all values are there
        Assertions.assertThat(readValues(table, PRODUCER_KEY)).isEqualTo(new ArrayList<>(producerValues));
        final TimeSeriesUpdaterProgress producerMarker = TimeSeriesUpdaterProgress
                .readUpdateProgress(producerLookup.getUpdateFinishedFile());
        Assertions.checkNotNull(producerMarker);
        Assertions.checkEquals(producer.getEpochId(), producerMarker.getEpochId());

        //the epoch's staged snapshot (start-time empty, then per-flush overwrites) mirrors the published generation
        final List<MemoryFileSummary> stagedFinal = TimeSeriesUpdateTransaction.readEpochIndexSnapshot(producerLookup,
                producer.getEpochId());
        Assertions.checkNotNull(stagedFinal, "the epoch index snapshot must exist");
        Assertions.checkEquals(readPublishedSummaries(producerLookup).size(), stagedFinal.size(),
                "the final snapshot must mirror the published generation's file count");
    }

    /**
     * M2 adoption for an in-tree ancestor-owned pin (NOT self-served): a live update attempt (epoch) of the producer
     * key runs on another thread, its published store is empty, and the demanding thread's wait would close a cycle
     * that lies inside its own demand tree. The demand then adopts the staged epoch snapshot (updatedTo = highest
     * staged firstValueEndTime, the staged values readable on the demanding thread), records the consumed
     * '&lt;key&gt;#&lt;epochId&gt;' ledger into its own attempt's marker, and nothing epoch-side was published.
     */
    @Test
    public void testInTreeDemandAdoptsStagedEpochSnapshotInsteadOfEmptyStore() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("epochVisibility");
        final TimeSeriesLookupStorageCache<String, FDate> producerLookup = table.getLookupTableCache(PRODUCER_KEY);
        final List<FDate> producerValues = newValues(1, 8);

        final DemandObservations observations = new DemandObservations();
        final String producerGraphKey = WaitForGraph.newKey(table.getName(), PRODUCER_KEY);
        final String consumerGraphKey = WaitForGraph.newKey(table.getName(), CONSUMER_KEY);
        final CountDownLatch stagedAndPinned = new CountDownLatch(1);
        final CountDownLatch demandCompleted = new CountDownLatch(1);
        final AtomicReference<String> liveEpochId = new AtomicReference<>();
        final AtomicReference<Throwable> producerFailure = new AtomicReference<>();

        //the producer attempt on its own thread: heartbeat pin + progress marker naming a live epoch + a staged
        //snapshot of two flushed batches; it never commits (close without commit discards the index), and it 'waits'
        //on the consumer key inside the wait-for graph, which is what closes the cycle the demand detects in-tree
        final Thread producerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                final File progressFile = producerLookup.getUpdateProgressFile();
                try (HeartbeatFileChannelLock pin = new HeartbeatFileChannelLock(
                        new File(progressFile.getAbsolutePath() + ".lock"));
                        TimeSeriesUpdateTransaction<FDate> stagedTransaction = producerLookup
                                .newUpdateTransaction(true)) {
                    if (!pin.tryLock()) {
                        throw new IllegalStateException("fixture could not acquire the producer update pin");
                    }
                    WaitForGraph.pinAcquired(producerGraphKey);
                    SequentialChunkedUpdateProgress.doUpdate(stagedTransaction,
                            new EpochAwareInternalMethods(table, producerLookup,
                                    stagedTransaction.getEpochLocalDataId()),
                            stagedTransaction.getPrecedingMemoryOffset(), stagedTransaction.getMemoryOffset(),
                            stagedTransaction.getPrecedingValueCount(),
                            WrapperCloseableIterable.maybeWrap(producerValues));
                    stagedTransaction.writeEpochIndexSnapshot();
                    liveEpochId.set(stagedTransaction.getEpochLocalDataId());
                    TimeSeriesUpdaterProgress.writeUpdateProgress(progressFile, FDate.now(), 2,
                            producerValues.size(), producerValues.get(0),
                            producerValues.get(producerValues.size() - 1), liveEpochId.get(), null);
                    WaitForGraph.beginWait(consumerGraphKey);
                    stagedAndPinned.countDown();
                    if (!demandCompleted.await(60, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("demand side never completed");
                    }
                    WaitForGraph.endWait(consumerGraphKey);
                } catch (final Throwable t) {
                    producerFailure.set(t);
                } finally {
                    WaitForGraph.pinReleased(producerGraphKey, Thread.currentThread());
                    stagedAndPinned.countDown();
                }
            }
        }, "epochProducerFixture");
        producerThread.start();
        Assertions.checkTrue(stagedAndPinned.await(30, TimeUnit.SECONDS), "the producer fixture never staged");
        Assertions.checkTrue(liveEpochId.get() != null, "the producer fixture must have minted an epoch id");
        if (producerFailure.get() != null) {
            throw new IllegalStateException("producer fixture failed", producerFailure.get());
        }

        try {
            //this thread serves the consumer 'task' submitted by the producer thread (same edges the lazy data updater
            //registers at submit/join) and inherits the producer epoch chain (same capture/install as ALazyDataUpdater)
            WaitForGraph.taskStarted(consumerGraphKey, producerThread);
            try (EpochScopes.EpochScope installedChain = EpochScopes.install(Arrays.asList(liveEpochId.get()))) {
                runNestedConsumerDemand(table, observations);
            } finally {
                WaitForGraph.taskFinished(consumerGraphKey);
            }
        } finally {
            demandCompleted.countDown();
            producerThread.join(TimeUnit.SECONDS.toMillis(60));
        }
        if (producerFailure.get() != null) {
            throw new IllegalStateException("producer fixture failed", producerFailure.get());
        }

        //the innermost demand adopted the staged snapshot instead of throwing
        final TimeSeriesUpdaterResult demandResult = observations.demandResult.get();
        Assertions.checkNotNull(demandResult, "the nested demand must have produced an adoption result");
        //two full batches staged: the highest firstValueEndTime among the staged summaries is the start of the second
        Assertions.checkEquals(producerValues.get(BATCH_FLUSH_INTERVAL), demandResult.getUpdatedTo(),
                "updatedTo must be the highest staged firstValueEndTime");

        //the snapshot seen at adoption time: exactly the two flushed summaries
        Assertions.checkNotNull(observations.stagedSnapshot.get(), "a staged epoch snapshot must exist");
        Assertions.checkEquals(2, observations.stagedSnapshot.get().size(),
                "two batches were flushed at the adoption point");

        //the immediate demand-side reads saw the staged values (count matches the staged batches)
        Assertions.checkNotNull(observations.demandViewValues.get());
        Assertions.assertThat(observations.demandViewValues.get())
                .isEqualTo(new ArrayList<>(producerValues.subList(0, 2 * BATCH_FLUSH_INTERVAL)));

        //the published generation stayed empty at adoption (in-epoch visibility, nothing was published by it)
        Assertions.checkNotNull(observations.publishedSummariesAtAdoption.get());
        Assertions.checkTrue(observations.publishedSummariesAtAdoption.get().isEmpty(),
                "the adopted state must not have become a published generation: "
                        + observations.publishedSummariesAtAdoption.get());

        //the discarded producer attempt published nothing: the store's published index is still empty on disk
        Assertions.checkTrue(readPublishedSummaries(producerLookup).isEmpty(),
                "the discarded producer attempt must not have published anything");

        //the epoch snapshot lives in the non-numeric staging directory, outside the version's data directory
        final File stagingDir = TimeSeriesUpdateTransaction.getEpochStagingDirectory(producerLookup,
                liveEpochId.get());
        Assertions.checkEquals("epoch-staging", stagingDir.getParentFile().getName());
        final File snapshotFile = new File(stagingDir, TimeSeriesUpdateTransaction.EPOCH_INDEX_SNAPSHOT_FILE_NAME);
        Assertions.checkTrue(snapshotFile.exists(), "the epoch index snapshot file must exist: " + snapshotFile);

        //the consumer attempt published its own store from the adopted values and recorded the consumed epoch ledger
        final List<FDate> publishedConsumerValues = readValues(table, CONSUMER_KEY);
        Assertions.assertThat(publishedConsumerValues)
                .isEqualTo(new ArrayList<>(producerValues.subList(0, BATCH_FLUSH_INTERVAL)));
        final TimeSeriesUpdaterProgress consumerMarker = TimeSeriesUpdaterProgress
                .readUpdateProgress(observations.consumerUpdater.get().getUpdateFinishedFile());
        Assertions.checkNotNull(consumerMarker, "the consumer attempt must have left its finished marker");
        Assertions.checkEquals(observations.consumerUpdater.get().getEpochId(), consumerMarker.getEpochId());
        Assertions.checkEquals(Arrays.asList(PRODUCER_KEY + "#" + liveEpochId.get()),
                consumerMarker.getConsumedEpochEntries());
    }

    /**
     * The 'Adoption found empty store' refusal must keep firing, unchanged, for a demand that breaks its pin wait on
     * a FINISHED marker of a foreign holder while the published store is empty: no self-served shortcut, no
     * unverifiable epoch adoption - plain retryable without touching the store.
     */
    @Test
    public void testForeignEmptyAdoptionRefusalStillFiresUnchanged() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("epochForeignRefusal");
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
                            //immediate demand-side reads: the adopted epoch view serves the staged values (and only
                            //an adoption makes them visible - a self-served skip reads the empty published state)
                            observations.demandViewValues.set(readValues(table, PRODUCER_KEY));
                            observations.publishedSummariesAtAdoption.set(
                                    readPublishedSummaries(table.getLookupTableCache(PRODUCER_KEY)));
                            observations.stagedSnapshot.set(TimeSeriesUpdateTransaction.readEpochIndexSnapshot(
                                    table.getLookupTableCache(PRODUCER_KEY), readMarkerEpochId(table)));
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

        //the aborted second attempt changed nothing observable - and since M3-cp it also reclaimed its own epoch
        //artifacts (data files and staging directory) on the abort path
        Assertions.assertThat(readValues(table, PRODUCER_KEY)).isEqualTo(newValues(1, 4));
        final List<MemoryFileSummary> published = readPublishedSummaries(producerLookup);
        Assertions.checkFalse(published.isEmpty());
        final String abortedEpochId = producer.getEpochId();
        Assertions.checkNotNull(abortedEpochId);
        Assertions.checkFalse(TimeSeriesUpdateTransaction.getEpochStagingDirectory(producerLookup, abortedEpochId)
                .exists(), "the aborted attempt must have reclaimed its own staging directory");
        Assertions.checkTrue(findEpochDataFiles(producerLookup, abortedEpochId).isEmpty(),
                "the aborted attempt must have reclaimed its own epoch data files");
        //and every data file the surviving published generation references is still there with its bytes
        for (final MemoryFileSummary summary : published) {
            final File dataFile = new File(summary.getMemoryResourceUri());
            Assertions.checkTrue(dataFile.exists(), "the reclaim must not remove a referenced data file: " + dataFile);
        }
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

    private static List<File> findEpochDataFiles(final TimeSeriesLookupStorageCache<String, FDate> lookup,
            final String epochId) {
        final File dataDirectory = lookup.getDirectoryHashKeyVersionMemory()
                .getDirectoryHashKeyVersionDataShared();
        final File[] files = dataDirectory.listFiles((dir, name) -> name.startsWith("memory_" + epochId));
        final List<File> found = new ArrayList<>();
        if (files != null) {
            for (final File file : files) {
                found.add(file);
            }
        }
        return found;
    }

    /**
     * Stands in for a live in-progress attempt's internal methods: it knows its epoch id, so the progress writer
     * files its batches epoch local exactly like a real epoch-scoped attempt would.
     */
    @NotThreadSafe
    private static final class EpochAwareInternalMethods implements ITimeSeriesUpdaterInternalMethods<String, FDate> {

        private final ATimeSeriesDB<String, FDate> table;
        private final TimeSeriesLookupStorageCache<String, FDate> lookupTable;
        private final String epochLocalDataId;

        private EpochAwareInternalMethods(final ATimeSeriesDB<String, FDate> table,
                final TimeSeriesLookupStorageCache<String, FDate> lookupTable, final String epochLocalDataId) {
            this.table = table;
            this.lookupTable = lookupTable;
            this.epochLocalDataId = epochLocalDataId;
        }

        @Override
        public ISerde<FDate> getValueSerde() {
            return table.getValueSerde();
        }

        @Override
        public TimeSeriesLookupStorageCache<String, FDate> getLookupTable() {
            return lookupTable;
        }

        @Override
        public ITimeSeriesDB<String, FDate> getTable() {
            return table;
        }

        @Override
        public FDate extractStartTime(final FDate element) {
            return element;
        }

        @Override
        public FDate extractEndTime(final FDate element) {
            return element;
        }

        @Override
        public void onElement(final ITimeSeriesUpdateProgress updateProgress, final long pendingCount) {}

        @Override
        public void onFlush(final ITimeSeriesUpdateProgress updateProgress, final long flushIndex) {}

        @Override
        public String getKey() {
            return PRODUCER_KEY;
        }

        @Override
        public boolean shouldRedoLastFile() {
            return true;
        }

        @Override
        public String getEpochLocalDataId() {
            return epochLocalDataId;
        }
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
