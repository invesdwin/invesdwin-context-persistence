package de.invesdwin.context.persistence.leveldb.timeseries;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.FileUtils;

import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ACloseableIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.concurrent.AParallelChunkConsumerIterator;
import de.invesdwin.util.collections.iterable.concurrent.ProducerQueueIterable;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import ezdb.serde.Serde;

@NotThreadSafe
public abstract class ATimeSeriesUpdater<K, V> {

    public static final int BATCH_FLUSH_INTERVAL = 10_000;
    public static final int BATCH_QUEUE_SIZE = 500_000 / BATCH_FLUSH_INTERVAL;
    public static final int BATCH_WRITER_THREADS = Executors.getCpuThreadPoolCount();

    private final Serde<V> valueSerde;
    private final ATimeSeriesDB<K, V> table;
    private final TimeSeriesFileLookupTableCache<K, V> lookupTable;
    private final File updateLockFile;

    private final K key;
    private FDate minTime = null;
    private FDate maxTime = null;
    private int count = 0;

    public ATimeSeriesUpdater(final K key, final ATimeSeriesDB<K, V> table) {
        this.key = key;
        this.valueSerde = table.getValueSerde();
        this.table = table;
        this.lookupTable = table.getLookupTableCache(key);
        this.updateLockFile = lookupTable.getUpdateLockFile();
    }

    public FDate getMinTime() {
        return minTime;
    }

    public FDate getMaxTime() {
        return maxTime;
    }

    public int getCount() {
        return count;
    }

    public final boolean update() throws IncompleteUpdateFoundException {
        try {
            if (!table.getTableLock(key).writeLock().tryLock(1, TimeUnit.MINUTES)) {
                throw new RetryLaterRuntimeException("Write lock could not be acquired for table [" + table.getName()
                        + "] and key [" + key + "]. Please ensure all iterators are closed!");
            }
        } catch (final InterruptedException e1) {
            throw new RuntimeException(e1);
        }
        try {
            if (updateLockFile.exists()) {
                throw new IncompleteUpdateFoundException("Incomplete update found for table [" + table.getName()
                        + "], need to clean everything up to restore all from scratch.");
            }
            try {
                FileUtils.touch(updateLockFile);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            final Instant updateStart = new Instant();
            onUpdateStart();
            doUpdate();
            onUpdateFinished(updateStart);
            Assertions.assertThat(updateLockFile.delete()).isTrue();
            return true;
        } finally {
            table.getTableLock(key).writeLock().unlock();
        }
    }

    private void doUpdate() {
        try (final ICloseableIterator<? extends V> elements = getSource().iterator()) {
            final ICloseableIterator<UpdateProgress> batchWriterProducer = new ICloseableIterator<UpdateProgress>() {

                @Override
                public boolean hasNext() {
                    return elements.hasNext();
                }

                @Override
                public UpdateProgress next() {
                    final UpdateProgress progress = new UpdateProgress();
                    while (elements.hasNext()) {
                        final V element = elements.next();
                        if (progress.onElement(element)) {
                            return progress;
                        }
                    }
                    return progress;
                }

                @Override
                public void close() {
                    elements.close();
                }
            };
            //do IO in a different thread than batch filling
            try (final ACloseableIterator<UpdateProgress> batchProducer = new ProducerQueueIterable<UpdateProgress>(
                    getClass().getSimpleName() + "_batchProducer_" + table.getDatabaseName(key),
                    () -> batchWriterProducer, BATCH_QUEUE_SIZE).iterator()) {
                final AtomicInteger flushIndex = new AtomicInteger();
                try (final ACloseableIterator<UpdateProgress> parallelConsumer = new AParallelChunkConsumerIterator<UpdateProgress, UpdateProgress>(
                        getClass().getSimpleName() + "_batchConsumer_" + table.getDatabaseName(key), batchProducer,
                        BATCH_WRITER_THREADS) {

                    @Override
                    protected UpdateProgress doWork(final UpdateProgress request) {
                        request.write(flushIndex.incrementAndGet());
                        return request;
                    }
                }) {
                    while (parallelConsumer.hasNext()) {
                        final UpdateProgress progress = parallelConsumer.next();
                        count += progress.getCount();
                        if (minTime == null) {
                            minTime = progress.getMinTime();
                        }
                        maxTime = progress.getMaxTime();
                    }
                }
            }

        }
    }

    protected abstract ICloseableIterable<? extends V> getSource();

    protected abstract void onUpdateFinished(Instant updateStart);

    protected abstract void onUpdateStart();

    protected abstract FDate extractTime(V element);

    protected abstract FDate extractEndTime(V element);

    protected abstract void onFlush(int flushIndex, Instant flushStart, UpdateProgress updateProgress);

    public class UpdateProgress {

        private final List<V> batch = new ArrayList<V>(BATCH_FLUSH_INTERVAL);
        private int count;
        private FDate minTime;
        private FDate maxTime;

        public FDate getMinTime() {
            return minTime;
        }

        public FDate getMaxTime() {
            return maxTime;
        }

        public int getCount() {
            return count;
        }

        private boolean onElement(final V element) {
            final FDate time = extractTime(element);
            if (minTime == null) {
                minTime = time;
            }
            if (maxTime != null && maxTime.isAfterOrEqualTo(time)) {
                throw new IllegalArgumentException(
                        "New element time [" + time + "] is not after previous element time [" + maxTime + "]");
            }
            final FDate endTime = extractEndTime(element);
            maxTime = endTime;
            batch.add(element);
            count++;
            return getCount() % BATCH_FLUSH_INTERVAL == 0;
        }

        @SuppressWarnings("null")
        private void write(final int flushIndex) {
            final Instant flushStart = new Instant();

            final File newFile = lookupTable.newFile(minTime);
            final SerializingCollection<V> collection = new SerializingCollection<V>(newFile, false) {
                @Override
                protected V fromBytes(final byte[] bytes) {
                    throw new UnsupportedOperationException();
                }

                @Override
                protected byte[] toBytes(final V element) {
                    return valueSerde.toBytes(element);
                }

                @Override
                protected Integer getFixedLength() {
                    return table.getFixedLength();
                }

            };
            V firstElement = null;
            V lastElement = null;
            for (final V element : batch) {
                collection.add(element);
                if (firstElement == null) {
                    firstElement = element;
                }
                lastElement = element;
            }
            collection.close();
            lookupTable.finishFile(minTime, firstElement, lastElement);

            onFlush(flushIndex, flushStart, this);
        }

    }

}
