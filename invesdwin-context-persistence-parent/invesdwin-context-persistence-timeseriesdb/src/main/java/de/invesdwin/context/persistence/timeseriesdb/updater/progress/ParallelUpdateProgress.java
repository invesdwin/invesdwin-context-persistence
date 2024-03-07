package de.invesdwin.context.persistence.timeseriesdb.updater.progress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.persistence.timeseriesdb.SerializingCollection;
import de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.util.collections.iterable.ACloseableIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.concurrent.AParallelChunkConsumerIterator;
import de.invesdwin.util.collections.iterable.concurrent.ProducerQueueIterable;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.OperatingSystem;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.file.SegmentedMemoryMappedFile;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class ParallelUpdateProgress<K, V> implements IUpdateProgress<K, V> {

    private static final int WRITER_THREADS = Executors.getCpuThreadPoolCount();
    private static final WrappedExecutorService WRITER_LIMIT_EXECUTOR = Executors
            .newFixedThreadPool(ParallelUpdateProgress.class.getSimpleName() + "_WRITER_LIMIT", WRITER_THREADS)
            .setDynamicThreadName(false);

    private ITimeSeriesUpdaterInternalMethods<K, V> parent;
    private TextDescription name;
    private File tempFile;
    private int valueCount;
    private V firstElement;
    private FDate minTime;
    private V lastElement;
    private FDate maxTime;
    private final Object[] batch = new Object[ATimeSeriesUpdater.BATCH_FLUSH_INTERVAL];

    public void init(final ITimeSeriesUpdaterInternalMethods<K, V> parent, final File tempFile) {
        this.parent = parent;
        this.name = new TextDescription("%s[%s]: write", ATimeSeriesUpdater.class.getSimpleName(), parent.getKey());
        this.tempFile = tempFile;
    }

    public void reset() {
        parent = null;
        name = null;
        tempFile = null;
        valueCount = 0;
        firstElement = null;
        minTime = null;
        lastElement = null;
        maxTime = null;
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

    public boolean onElement(final V element, final FDate startTime, final FDate endTime) {
        if (firstElement == null) {
            firstElement = element;
            minTime = endTime;
        }
        if (maxTime != null) {
            if (maxTime.isAfterNotNullSafe(startTime)) {
                throw new IllegalArgumentException("New element startTime [" + startTime
                        + "] is not after or equal to previous element endTime [" + maxTime + "] for table ["
                        + parent.getTable().getName() + "] and key [" + parent.getKey() + "]");
            }
        }
        if (startTime.isAfterNotNullSafe(endTime)) {
            throw new IllegalArgumentException(
                    "New element endTime [" + endTime + "] is not after or equal to element startTime [" + startTime
                            + "] for table [" + parent.getTable().getName() + "] and key [" + parent.getKey() + "]");
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
    }

    public void transferToMemoryFile(final FileOutputStream memoryFileOut, final String memoryResourceUri,
            final long precedingMemoryOffset, final long memoryOffset, final int flushIndex,
            final long precedingValueCount) {
        try (FileInputStream tempIn = new FileInputStream(tempFile)) {
            final long tempFileLength = tempFile.length();
            long remaining = tempFileLength;
            long position = 0;
            while (remaining > 0L) {
                final long copied = tempIn.getChannel().transferTo(position, remaining, memoryFileOut.getChannel());
                remaining -= copied;
                position += copied;
            }
            //close first so that lz4 writes out its footer bytes (a flush is not sufficient)
            parent.getLookupTable()
                    .finishFile(minTime, firstElement, lastElement, precedingValueCount, valueCount, memoryResourceUri,
                            precedingMemoryOffset, memoryOffset, tempFileLength);
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
                public V fromBuffer(final IByteBuffer buffer) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public int toBuffer(final IByteBuffer buffer, final V obj) {
                    return parent.getValueSerde().toBuffer(buffer, obj);
                }
            };
        }

        @Override
        protected ICompressionFactory getCompressionFactory() {
            return parent.getTable().getCompressionFactory();
        }

        @Override
        protected OutputStream newCompressor(final OutputStream out) {
            return getCompressionFactory().newCompressor(out, ATimeSeriesUpdater.LARGE_COMPRESSOR);
        }

        @Override
        protected Integer newFixedLength() {
            return parent.getTable().getValueFixedLength();
        }

    }

    public V getFirstElement() {
        return firstElement;
    }

    public static <K, V> void doUpdate(final ITimeSeriesUpdaterInternalMethods<K, V> parent,
            final long initialPrecedingMemoryOffset, final long initialMemoryOffset,
            final long initialPrecedingValueCount, final ICloseableIterable<? extends V> source) {
        final File memoryFile = parent.getLookupTable().getMemoryFile();
        final File tempDir = new File(memoryFile.getParentFile(), ATimeSeriesUpdater.class.getSimpleName());
        Files.deleteQuietly(tempDir);
        try {
            Files.forceMkdir(tempDir);
        } catch (final IOException e1) {
            throw new RuntimeException(e1);
        }

        final AtomicInteger progressIndex = new AtomicInteger();

        try (ICloseableIterator<ParallelUpdateProgress<K, V>> batchWriterProducer = new ICloseableIterator<ParallelUpdateProgress<K, V>>() {

            private final ICloseableIterator<? extends V> elements = source.iterator();

            @Override
            public boolean hasNext() {
                return elements.hasNext();
            }

            @SuppressWarnings("unchecked")
            @Override
            public ParallelUpdateProgress<K, V> next() {
                final File tempFile = new File(tempDir, progressIndex.incrementAndGet() + ".data");
                final ParallelUpdateProgress<K, V> progress = ParallelUpdateProgressPool.INSTANCE.borrowObject();
                progress.init(parent, tempFile);
                try {
                    while (true) {
                        final V element = elements.next();
                        final FDate startTime = parent.extractStartTime(element);
                        final FDate endTime = parent.extractEndTime(element);
                        if (progress.onElement(element, startTime, endTime)) {
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
                        WRITER_THREADS, WRITER_LIMIT_EXECUTOR) {
                    @Override
                    protected ParallelUpdateProgress<K, V> doWork(final ParallelUpdateProgress<K, V> request) {
                        request.writeToTempFile();
                        return request;
                    }
                }) {
                    flush(parent, initialPrecedingMemoryOffset, initialMemoryOffset, initialPrecedingValueCount,
                            parallelConsumer);
                }
            }
            if (batchWriterProducer.hasNext()) {
                throw new IllegalStateException(
                        "there are still elements to be processed, but the parallel producer did not feed them");
            }
        }
        //clean up temp files (now that memory file out channel has been synced with filesystem)
        Files.deleteQuietly(tempDir);
    }

    public static File newMemoryFile(final ITimeSeriesUpdaterInternalMethods<?, ?> parent,
            final long precedingMemoryOffset) {
        final File memoryFile = parent.getLookupTable().getMemoryFile();
        return newMemoryFile(memoryFile, precedingMemoryOffset);
    }

    public static File newMemoryFile(final File memoryFile, final long precedingMemoryOffset) {
        if (OperatingSystem.isWindows()) {
            return new File(memoryFile.getAbsolutePath() + "." + precedingMemoryOffset);
        } else {
            return memoryFile;
        }
    }

    private static <K, V> void flush(final ITimeSeriesUpdaterInternalMethods<K, V> parent,
            final long initialPrecedingMemoryOffset, final long initialMemoryOffset,
            final long initialPrecedingValueCount,
            final ICloseableIterator<ParallelUpdateProgress<K, V>> batchWriterProducer) {
        int flushIndex = 0;
        long precedingMemoryOffset = initialPrecedingMemoryOffset;
        long precedingValueCount = initialPrecedingValueCount;

        File memoryFile = newMemoryFile(parent, precedingMemoryOffset);

        FileOutputStream memoryFileOut = null;
        try {
            memoryFileOut = new FileOutputStream(memoryFile, true);
            if (initialMemoryOffset > 0L) {
                memoryFileOut.getChannel().position(initialMemoryOffset);
            }

            try {
                while (true) {
                    final ParallelUpdateProgress<K, V> progress = batchWriterProducer.next();
                    flushIndex++;
                    final long memoryOffset = memoryFileOut.getChannel().position();
                    progress.transferToMemoryFile(memoryFileOut, memoryFile.getAbsolutePath(), precedingMemoryOffset,
                            memoryOffset, flushIndex, precedingValueCount);
                    precedingValueCount += progress.getValueCount();

                    if (OperatingSystem.isWindows()
                            && memoryOffset > SegmentedMemoryMappedFile.WINDOWS_MAX_LENGTH_PER_SEGMENT_DISK) {
                        precedingMemoryOffset += memoryOffset;
                        memoryFile = newMemoryFile(parent, precedingMemoryOffset);
                        memoryFileOut = new FileOutputStream(memoryFile, true);
                    }

                    ParallelUpdateProgressPool.INSTANCE.returnObject(progress);
                }
            } catch (final NoSuchElementException e) {
                //end reached
            }

            /*
             * force sync on filesystem:
             * https://stackoverflow.com/questions/52481281/does-java-nio-file-files-copy-call-sync-on-the-file-system
             */
            //            memoryFileOut.getChannel().force(true);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (memoryFileOut != null) {
                try {
                    memoryFileOut.close();
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

}