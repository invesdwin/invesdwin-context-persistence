package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.persistence.ezdb.table.range.ADelegateRangeTable;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateFoundException;
import de.invesdwin.context.persistence.timeseriesdb.PrepareForUpdateResult;
import de.invesdwin.context.persistence.timeseriesdb.SerializingCollection;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesStorageCache;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.FlatteningIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.lock.FileChannelLock;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.Locks;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.description.TextDescription;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.pool.buffered.BufferedFileDataOutputStream;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public abstract class ATimeSeriesUpdater<K, V> implements ITimeSeriesUpdater<K, V> {

    public static final int BATCH_FLUSH_INTERVAL = ADelegateRangeTable.BATCH_FLUSH_INTERVAL;
    public static final int BATCH_QUEUE_SIZE = 500_000 / BATCH_FLUSH_INTERVAL;
    public static final int BATCH_WRITER_THREADS = Executors.getCpuThreadPoolCount();
    public static final boolean LARGE_COMPRESSOR = true;

    private final ISerde<V> valueSerde;
    private final ATimeSeriesDB<K, V> table;
    private final TimeSeriesStorageCache<K, V> lookupTable;
    private final File updateLockFile;

    private final K key;
    private volatile FDate minTime = null;
    private volatile FDate maxTime = null;
    private int count = 0;

    public ATimeSeriesUpdater(final K key, final ATimeSeriesDB<K, V> table) {
        if (key == null) {
            throw new NullPointerException("key should not be null");
        }
        this.key = key;
        this.valueSerde = table.getValueSerde();
        this.table = table;
        this.lookupTable = table.getLookupTableCache(key);
        this.updateLockFile = lookupTable.getUpdateLockFile();
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public FDate getMinTime() {
        return minTime;
    }

    @Override
    public FDate getMaxTime() {
        return maxTime;
    }

    public int getCount() {
        return count;
    }

    @Override
    public final boolean update() throws IncompleteUpdateFoundException {
        final ILock writeLock = table.getTableLock(key).writeLock();
        try {
            if (!writeLock.tryLock(1, TimeUnit.MINUTES)) {
                throw Locks.getLockTrace()
                        .handleLockException(writeLock.getName(),
                                new RetryLaterRuntimeException(
                                        "Write lock could not be acquired for table [" + table.getName() + "] and key ["
                                                + key + "]. Please ensure all iterators are closed!"));
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        final File updateLockSyncFile = new File(updateLockFile.getAbsolutePath() + ".sync");
        try (FileChannelLock updateLockSyncFileLock = new FileChannelLock(updateLockSyncFile)) {
            if (updateLockSyncFile.exists() || !updateLockSyncFileLock.tryLock() || updateLockFile.exists()) {
                throw new IncompleteUpdateFoundException("Incomplete update found for table [" + table.getName()
                        + "] and key [" + key + "], need to clean everything up to restore all from scratch.");
            }
            try {
                try {
                    Files.touch(updateLockFile);
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
                final Instant updateStart = new Instant();
                onUpdateStart();
                doUpdate();
                onUpdateFinished(updateStart);
                Assertions.assertThat(updateLockFile.delete()).isTrue();
                return true;
            } catch (final Throwable t) {
                final IncompleteUpdateFoundException incompleteException = Throwables.getCauseByType(t,
                        IncompleteUpdateFoundException.class);
                if (incompleteException != null) {
                    throw incompleteException;
                } else {
                    throw new IncompleteUpdateFoundException("Something unexpected went wrong", t);
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void doUpdate() {
        final PrepareForUpdateResult<V> prepareForUpdateResult = lookupTable.prepareForUpdate(shouldRedoLastFile());
        final FDate updateFrom = prepareForUpdateResult.getUpdateFrom();
        final List<V> lastValues = prepareForUpdateResult.getLastValues();
        final long initialAddressOffset = prepareForUpdateResult.getAddressOffset();

        final ICloseableIterable<? extends V> source = getSource(updateFrom);
        final FlatteningIterable<? extends V> flatteningSources = new FlatteningIterable<>(lastValues, source);
        try (ICloseableIterator<UpdateProgress> batchWriterProducer = new ICloseableIterator<UpdateProgress>() {

            private final UpdateProgress progress = new UpdateProgress(initialAddressOffset);
            private final ICloseableIterator<? extends V> elements = flatteningSources.iterator();

            @Override
            public boolean hasNext() {
                return elements.hasNext();
            }

            @Override
            public UpdateProgress next() {
                progress.reset();
                try {
                    while (true) {
                        final V element = elements.next();
                        final FDate endTime = extractEndTime(element);
                        if (updateFrom != null) {
                            if (endTime.isBeforeNotNullSafe(updateFrom)) {
                                //ensure we add no duplicate values
                                continue;
                            }
                        }
                        if (progress.onElement(element, endTime)) {
                            return progress;
                        }
                    }
                } catch (NoSuchElementException e) {
                    //end reached
                    if (progress.firstElement == null) {
                        throw e;
                    }
                }
                return progress;
            }

            @Override
            public void close() {
                elements.close();
                progress.close();
            }
        }) {
            flush(batchWriterProducer);
        }

    }

    private void flush(final ICloseableIterator<UpdateProgress> batchWriterProducer) {
        int flushIndex = 0;
        try {
            while (true) {
                final UpdateProgress progress = batchWriterProducer.next();
                progress.write(flushIndex++);
                count += progress.getValueCount();
                if (minTime == null) {
                    minTime = progress.getMinTime();
                }
                maxTime = progress.getMaxTime();
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
    }

    protected boolean shouldRedoLastFile() {
        return true;
    }

    protected abstract ICloseableIterable<? extends V> getSource(FDate updateFrom);

    protected abstract void onUpdateFinished(Instant updateStart);

    protected abstract void onUpdateStart();

    protected abstract FDate extractEndTime(V element);

    protected abstract void onFlush(int flushIndex, UpdateProgress updateProgress);

    public class UpdateProgress implements Closeable {

        private final TextDescription name = new TextDescription("%s[%s]: write",
                ATimeSeriesUpdater.class.getSimpleName(), key);

        private long memoryOffset;
        private final File memoryFile;
        private final String memoryFilePath;
        private int valueCount;
        private V firstElement;
        private FDate minTime;
        private V lastElement;
        private FDate maxTime;
        private ConfiguredSerializingCollection collection;
        private BufferedFileDataOutputStream out;

        public UpdateProgress(final long initialAddressOffset) {
            this.memoryOffset = initialAddressOffset;
            this.memoryFile = lookupTable.getMemoryFile();
            this.memoryFilePath = memoryFile.getAbsolutePath();
            try {
                this.out = new BufferedFileDataOutputStream(memoryFile);
                if (initialAddressOffset > 0L) {
                    this.out.seek(initialAddressOffset);
                }
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

        public FDate getMinTime() {
            return minTime;
        }

        public void reset() {
            this.valueCount = 0;
            this.firstElement = null;
            this.minTime = null;
            this.lastElement = null;
            this.maxTime = null;
            this.collection = null;
        }

        public FDate getMaxTime() {
            return maxTime;
        }

        public int getValueCount() {
            return valueCount;
        }

        private boolean onElement(final V element, final FDate endTime) {
            if (firstElement == null) {
                firstElement = element;
                minTime = endTime;
                collection = new ConfiguredSerializingCollection();
            }
            if (maxTime != null && maxTime.isAfterNotNullSafe(endTime)) {
                throw new IllegalArgumentException(
                        "New element end time [" + endTime + "] is not after or equal to previous element end time ["
                                + maxTime + "] for table [" + table.getName() + "] and key [" + key + "]");
            }
            maxTime = endTime;
            lastElement = element;
            collection.add(element);
            valueCount++;
            return valueCount % BATCH_FLUSH_INTERVAL == 0;
        }

        private void write(final int flushIndex) {
            try {
                //close first so that lz4 writes out its footer bytes (a flush is not sufficient)
                collection.close();
                final long memoryLength = out.position() - memoryOffset;
                lookupTable.finishFile(minTime, firstElement, lastElement, valueCount, memoryFilePath, memoryOffset,
                        memoryLength);
                memoryOffset += memoryLength;

                onFlush(flushIndex, this);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            if (out != null) {
                try {
                    out.close();
                    out = null;
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private final class ConfiguredSerializingCollection extends SerializingCollection<V> {

            private ConfiguredSerializingCollection() {
                super(name, memoryFile, false);
            }

            @Override
            protected ISerde<V> newSerde() {
                return new ISerde<V>() {

                    @Override
                    public V fromBytes(final byte[] bytes) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public byte[] toBytes(final V obj) {
                        return valueSerde.toBytes(obj);
                    }

                    @Override
                    public V fromBuffer(final IByteBuffer buffer, final int length) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public int toBuffer(final IByteBuffer buffer, final V obj) {
                        return valueSerde.toBuffer(buffer, obj);
                    }
                };
            }

            @Override
            protected OutputStream newCompressor(final OutputStream out) {
                return table.getCompressionFactory().newCompressor(out, LARGE_COMPRESSOR);
            }

            @Override
            protected InputStream newDecompressor(final InputStream inputStream) {
                return table.getCompressionFactory().newDecompressor(inputStream);
            }

            @Override
            protected OutputStream newFileOutputStream(final File file) throws IOException {
                return out.asNonClosing();
            }

            @Override
            protected Integer getFixedLength() {
                return table.getValueFixedLength();
            }

        }

    }

}
