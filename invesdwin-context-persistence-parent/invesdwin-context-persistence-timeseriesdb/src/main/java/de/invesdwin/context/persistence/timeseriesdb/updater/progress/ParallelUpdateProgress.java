package de.invesdwin.context.persistence.timeseriesdb.updater.progress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.SerializingCollection;
import de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.util.collections.iterable.ACloseableIterator;
import de.invesdwin.util.collections.iterable.FlatteningIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.concurrent.AParallelChunkConsumerIterator;
import de.invesdwin.util.collections.iterable.concurrent.ProducerQueueIterable;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.description.TextDescription;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class ParallelUpdateProgress<K, V> implements IUpdateProgress<K, V> {

    public static final int BATCH_WRITER_THREADS = Executors.getCpuThreadPoolCount();

    private final ITimeSeriesUpdaterInternalMethods<K, V> parent;
    private final TextDescription name;
    private final File tempFile;
    private int valueCount;
    private V firstElement;
    private FDate minTime;
    private V lastElement;
    private FDate maxTime;
    private Object[] batch;

    public ParallelUpdateProgress(final ITimeSeriesUpdaterInternalMethods<K, V> parent, final File tempFile) {
        this.parent = parent;
        this.name = new TextDescription("%s[%s]: write", ATimeSeriesUpdater.class.getSimpleName(), parent.getKey());
        this.tempFile = tempFile;
    }

    @Override
    public FDate getMinTime() {
        return minTime;
    }

    @Override
    public FDate getMaxTime() {
        return maxTime;
    }

    @Override
    public int getValueCount() {
        return valueCount;
    }

    public boolean onElement(final V element, final FDate endTime) {
        if (firstElement == null) {
            firstElement = element;
            minTime = endTime;
            batch = TimeseriesUpdaterBatchArrayPool.INSTANCE.borrowObject();
        }
        if (maxTime != null && maxTime.isAfterNotNullSafe(endTime)) {
            throw new IllegalArgumentException("New element end time [" + endTime
                    + "] is not after or equal to previous element end time [" + maxTime + "] for table ["
                    + parent.getTable().getName() + "] and key [" + parent.getKey() + "]");
        }
        maxTime = endTime;
        lastElement = element;
        batch[valueCount] = element;
        valueCount++;
        return valueCount % ATimeSeriesUpdater.BATCH_FLUSH_INTERVAL == 0;
    }

    @SuppressWarnings("unchecked")
    public void writeToTempFile() {
        final ConfiguredSerializingCollection collection = new ConfiguredSerializingCollection(tempFile);
        for (int i = 0; i < valueCount; i++) {
            collection.add((V) batch[i]);
            batch[i] = null;
        }
        collection.close();
        TimeseriesUpdaterBatchArrayPool.INSTANCE.returnObject(batch);
        batch = null;
    }

    public void transferToMemoryFile(final FileOutputStream memoryFileOut, final String memoryFilePath,
            final int flushIndex) {
        try (FileInputStream tempIn = new FileInputStream(tempFile)) {
            final long tempFileLength = tempFile.length();
            long remaining = tempFileLength;
            long position = 0;
            final long memoryOffset = memoryFileOut.getChannel().position();
            while (remaining > 0L) {
                final long copied = memoryFileOut.getChannel().transferFrom(tempIn.getChannel(), position, remaining);
                remaining -= copied;
                position += copied;
            }
            //close first so that lz4 writes out its footer bytes (a flush is not sufficient)
            parent.getLookupTable()
                    .finishFile(minTime, firstElement, lastElement, valueCount, memoryFilePath, memoryOffset,
                            tempFileLength);
            Files.deleteQuietly(tempFile);
            parent.onFlush(flushIndex, this);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private final class ConfiguredSerializingCollection extends SerializingCollection<V> {

        private ConfiguredSerializingCollection(final File tempFile) {
            super(name, tempFile, false);
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
                    return parent.getValueSerde().toBytes(obj);
                }

                @Override
                public V fromBuffer(final IByteBuffer buffer, final int length) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public int toBuffer(final IByteBuffer buffer, final V obj) {
                    return parent.getValueSerde().toBuffer(buffer, obj);
                }
            };
        }

        @Override
        protected OutputStream newCompressor(final OutputStream out) {
            return parent.getTable().getCompressionFactory().newCompressor(out, ATimeSeriesUpdater.LARGE_COMPRESSOR);
        }

        @Override
        protected InputStream newDecompressor(final InputStream inputStream) {
            return parent.getTable().getCompressionFactory().newDecompressor(inputStream);
        }

        @Override
        protected Integer getFixedLength() {
            return parent.getTable().getValueFixedLength();
        }

    }

    public V getFirstElement() {
        return firstElement;
    }

    public static <K, V> void doUpdate(final ITimeSeriesUpdaterInternalMethods<K, V> parent, final List<V> lastValues,
            final long initialAddressOffset, final ICloseableIterable<? extends V> skippingSource) {
        final File memoryFile = parent.getLookupTable().getMemoryFile();
        final File tempDir = new File(memoryFile.getParentFile(), ATimeSeriesUpdater.class.getSimpleName());
        Files.deleteQuietly(tempDir);
        try {
            Files.forceMkdir(tempDir);
        } catch (final IOException e1) {
            throw new RuntimeException(e1);
        }

        final AtomicInteger progressIndex = new AtomicInteger();

        final FlatteningIterable<? extends V> flatteningSources = new FlatteningIterable<>(lastValues, skippingSource);
        try (ICloseableIterator<ParallelUpdateProgress<K, V>> batchWriterProducer = new ICloseableIterator<ParallelUpdateProgress<K, V>>() {

            private final ICloseableIterator<? extends V> elements = flatteningSources.iterator();

            @Override
            public boolean hasNext() {
                return elements.hasNext();
            }

            @Override
            public ParallelUpdateProgress<K, V> next() {
                final File tempFile = new File(tempDir, progressIndex.incrementAndGet() + ".data");
                final ParallelUpdateProgress<K, V> progress = new ParallelUpdateProgress<K, V>(parent, tempFile);
                try {
                    while (true) {
                        final V element = elements.next();
                        final FDate endTime = parent.extractEndTime(element);
                        if (progress.onElement(element, endTime)) {
                            return progress;
                        }
                    }
                } catch (final NoSuchElementException e) {
                    //end reached
                    if (progress.getFirstElement() == null) {
                        throw e;
                    }
                }
                return progress;
            }

            @Override
            public void close() {
                elements.close();
            }
        }) {
            final String name = parent.getTable().hashKeyToString(parent.getKey());
            try (ACloseableIterator<ParallelUpdateProgress<K, V>> batchProducer = new ProducerQueueIterable<ParallelUpdateProgress<K, V>>(
                    ParallelUpdateProgress.class.getSimpleName() + "_batchProducer_" + name, () -> batchWriterProducer,
                    ATimeSeriesUpdater.BATCH_QUEUE_SIZE).iterator()) {
                try (ACloseableIterator<ParallelUpdateProgress<K, V>> parallelConsumer = new AParallelChunkConsumerIterator<ParallelUpdateProgress<K, V>, ParallelUpdateProgress<K, V>>(
                        ParallelUpdateProgress.class.getSimpleName() + "_batchConsumer_" + name, batchProducer,
                        BATCH_WRITER_THREADS) {
                    @Override
                    protected ParallelUpdateProgress<K, V> doWork(final ParallelUpdateProgress<K, V> request) {
                        request.writeToTempFile();
                        return request;
                    }
                }) {
                    flush(parent, initialAddressOffset, parallelConsumer);
                }
            }
        }
        //clean up temp files
        Files.deleteQuietly(tempDir);
    }

    private static <K, V> void flush(final ITimeSeriesUpdaterInternalMethods<K, V> parent,
            final long initialAddressOffset,
            final ICloseableIterator<ParallelUpdateProgress<K, V>> batchWriterProducer) {
        int flushIndex = 0;

        final File memoryFile = parent.getLookupTable().getMemoryFile();
        final String memoryFilePath = memoryFile.getAbsolutePath();

        try (FileOutputStream memoryFileOut = new FileOutputStream(memoryFile, true)) {
            if (initialAddressOffset > 0L) {
                memoryFileOut.getChannel().position(initialAddressOffset);
            }

            try {
                while (true) {
                    final ParallelUpdateProgress<K, V> progress = batchWriterProducer.next();
                    flushIndex++;
                    progress.transferToMemoryFile(memoryFileOut, memoryFilePath, flushIndex);
                }
            } catch (final NoSuchElementException e) {
                //end reached
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

}