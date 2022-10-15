package de.invesdwin.context.persistence.timeseriesdb.updater.progress;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.SerializingCollection;
import de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.pool.buffered.BufferedFileDataOutputStream;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class SequentialUpdateProgress<K, V> implements IUpdateProgress<K, V>, Closeable {

    private final ITimeSeriesUpdaterInternalMethods<K, V> parent;
    private final TextDescription name;

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

    public SequentialUpdateProgress(final ITimeSeriesUpdaterInternalMethods<K, V> parent,
            final long initialAddressOffset) {
        this.parent = parent;
        this.name = new TextDescription("%s[%s]: write", ATimeSeriesUpdater.class.getSimpleName(), parent.getKey());
        this.memoryOffset = initialAddressOffset;
        this.memoryFile = parent.getLookupTable().getMemoryFile();
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

    @Override
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

    @Override
    public FDate getMaxTime() {
        return maxTime;
    }

    @Override
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
            throw new IllegalArgumentException("New element end time [" + endTime
                    + "] is not after or equal to previous element end time [" + maxTime + "] for table ["
                    + parent.getTable().getName() + "] and key [" + parent.getKey() + "]");
        }
        maxTime = endTime;
        lastElement = element;
        collection.add(element);
        valueCount++;
        return valueCount % ATimeSeriesUpdater.BATCH_FLUSH_INTERVAL == 0;
    }

    private void write(final int flushIndex) {
        try {
            //close first so that lz4 writes out its footer bytes (a flush is not sufficient)
            collection.close();
            final long memoryLength = out.position() - memoryOffset;
            parent.getLookupTable()
                    .finishFile(minTime, firstElement, lastElement, valueCount, memoryFilePath, memoryOffset,
                            memoryLength);
            memoryOffset += memoryLength;
            parent.onFlush(flushIndex, this);
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
        protected OutputStream newCompressor(final OutputStream out) {
            return parent.getTable().getCompressionFactory().newCompressor(out, ATimeSeriesUpdater.LARGE_COMPRESSOR);
        }

        @Override
        protected InputStream newDecompressor(final InputStream inputStream) {
            return parent.getTable().getCompressionFactory().newDecompressor(inputStream);
        }

        @Override
        protected OutputStream newFileOutputStream(final File file) throws IOException {
            return out.asNonClosing();
        }

        @Override
        protected Integer getFixedLength() {
            return parent.getTable().getValueFixedLength();
        }

    }

    public static <K, V> void doUpdate(final ITimeSeriesUpdaterInternalMethods<K, V> parent,
            final long initialAddressOffset, final ICloseableIterable<? extends V> source) {
        try (ICloseableIterator<SequentialUpdateProgress<K, V>> batchWriterProducer = new ICloseableIterator<SequentialUpdateProgress<K, V>>() {

            private final SequentialUpdateProgress<K, V> progress = new SequentialUpdateProgress<K, V>(parent,
                    initialAddressOffset);
            private final ICloseableIterator<? extends V> elements = source.iterator();

            @Override
            public boolean hasNext() {
                return elements.hasNext();
            }

            @Override
            public SequentialUpdateProgress<K, V> next() {
                progress.reset();
                try {
                    while (true) {
                        final V element = elements.next();
                        final FDate endTime = parent.extractEndTime(element);
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
            if (batchWriterProducer.hasNext()) {
                throw new IllegalStateException(
                        "there are still elements to be processed, but the parallel producer did not feed them");
            }
        }
    }

    private static <K, V> void flush(final ICloseableIterator<SequentialUpdateProgress<K, V>> batchWriterProducer) {
        int flushIndex = 0;
        try {
            while (true) {
                final SequentialUpdateProgress<K, V> progress = batchWriterProducer.next();
                progress.write(flushIndex++);
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
    }

}
