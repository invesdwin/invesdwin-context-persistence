package de.invesdwin.context.persistence.timeseriesdb.updater.progress;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.persistence.timeseriesdb.SerializingCollection;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.storage.MemoryFiles;
import de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.OperatingSystem;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.file.IMemoryMappedFile;
import de.invesdwin.util.streams.pool.buffered.BufferedFileDataOutputStream;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class SequentialChunkedUpdateProgress<K, V> implements IUpdateProgress<K, V>, Closeable {
    private final ITimeSeriesUpdaterInternalMethods<K, V> parent;
    private final TextDescription name;
    private final File tempFile;
    private final BufferedFileDataOutputStream tempOut;

    private long precedingMemoryOffset;
    private long memoryOffset;
    private long precedingValueCount;
    private File memoryFile;
    private int valueCount;
    private V firstElement;
    private FDate minTime;
    private V lastElement;
    private FDate maxTime;
    private BufferedFileDataOutputStream out;

    private final Object[] batch;

    public SequentialChunkedUpdateProgress(final ITimeSeriesUpdaterInternalMethods<K, V> parent,
            final long initialPrecedingMemoryOffset, final long initialMemoryOffset,
            final long initialPrecedingValueCount, final File tempDir) {
        this.parent = parent;
        this.name = new TextDescription("%s[%s]: write", ATimeSeriesUpdater.class.getSimpleName(), parent.getKey());
        this.precedingMemoryOffset = initialPrecedingMemoryOffset;
        this.memoryOffset = initialMemoryOffset;
        this.precedingValueCount = initialPrecedingValueCount;
        this.batch = new Object[parent.getLookupTable().getBatchFlushInterval()];
        this.tempFile = new File(tempDir, "temp.data");
        this.memoryFile = newMemoryFile();
        try {
            this.tempOut = new BufferedFileDataOutputStream(tempFile);
            this.out = new BufferedFileDataOutputStream(memoryFile);
            if (initialMemoryOffset > 0L) {
                this.out.seek(initialMemoryOffset);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private File newMemoryFile() {
        return TimeSeriesStorageCache.newMemoryFile(parent, precedingMemoryOffset);
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
    }

    @Override
    public FDate getMaxTime() {
        return maxTime;
    }

    @Override
    public int getValueCount() {
        return valueCount;
    }

    private boolean onElement(final V element, final FDate startTime, final FDate endTime) {
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
        parent.onElement(this);
        return valueCount == batch.length;
    }

    @SuppressWarnings("unchecked")
    private void write(final int flushIndex, final boolean complete) {
        if (valueCount == 0) {
            return;
        }
        try {
            tempOut.getChannel().truncate(0);
            tempOut.seek(0);
            final ConfiguredSerializingCollection collection = new ConfiguredSerializingCollection(tempFile, tempOut);
            for (int i = 0; i < valueCount; i++) {
                collection.add((V) batch[i]);
                batch[i] = null;
            }
            collection.close();
            tempOut.flush();

            final long tempFileLength = tempOut.position();

            if (complete) {
                if (IMemoryMappedFile.isSegmentSizeExceeded(memoryOffset + tempFileLength)) {
                    precedingMemoryOffset += memoryOffset;
                    memoryFile = newMemoryFile();
                    out.close();
                    if (OperatingSystem.isWindows() && IMemoryMappedFile.isSegmentSizeExceeded(tempFileLength)) {
                        throw new IllegalStateException("Cannot write temp file of length [" + tempFileLength
                                + "] to new memory file because it would exceed the maximum segment size of ["
                                + IMemoryMappedFile.MAX_SEGMENT_SIZE_WINDOWS + "] on Windows");
                    }
                    out = new BufferedFileDataOutputStream(memoryFile);
                    memoryOffset = 0;
                }

                transferToMemoryFile(tempFileLength);

                parent.getLookupTable()
                        .finishFile(minTime, firstElement, lastElement, precedingValueCount, valueCount, memoryFile,
                                precedingMemoryOffset, memoryOffset, tempFileLength);
                memoryOffset += tempFileLength;
                precedingValueCount += valueCount;
                parent.onFlush(flushIndex, this);

            } else {
                // Route incomplete segment to isolated standalone file
                precedingMemoryOffset += memoryOffset;
                memoryFile = MemoryFiles.newIncompleteMemoryFile(memoryFile, memoryOffset);
                out.close();
                if (OperatingSystem.isWindows() && IMemoryMappedFile.isSegmentSizeExceeded(tempFileLength)) {
                    throw new IllegalStateException("Cannot write temp file of length [" + tempFileLength
                            + "] to incomplete memory file because it would exceed the maximum segment size of ["
                            + IMemoryMappedFile.MAX_SEGMENT_SIZE_WINDOWS + "] on Windows");
                }
                out = new BufferedFileDataOutputStream(memoryFile);
                memoryOffset = 0;

                // finish file
                transferToMemoryFile(tempFileLength);

                parent.getLookupTable()
                        .finishFile(minTime, firstElement, lastElement, precedingValueCount, valueCount, memoryFile,
                                precedingMemoryOffset, memoryOffset, tempFileLength);
                precedingValueCount += valueCount;
                parent.onFlush(flushIndex, this);

                // close
                precedingMemoryOffset += tempFileLength;
                close();
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void transferToMemoryFile(final long tempFileLength) throws IOException {
        out.flush(); // Ensure target buffer is flushed before channel write
        tempOut.getChannel().position(0);
        long remaining = tempFileLength;
        long position = 0;
        while (remaining > 0L) {
            final long copied = tempOut.getChannel().transferTo(position, remaining, out.getChannel());
            remaining -= copied;
            position += copied;
        }
    }

    @Override
    public void close() {
        if (out != null) {
            try {
                out.close();
                out = null;
                memoryOffset = Long.MIN_VALUE;
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (tempOut != null) {
            try {
                tempOut.close();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private final class ConfiguredSerializingCollection extends SerializingCollection<V> {

        private final BufferedFileDataOutputStream targetOut;

        private ConfiguredSerializingCollection(final File file, final BufferedFileDataOutputStream targetOut) {
            super(name, file, false);
            this.targetOut = targetOut;
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
        protected OutputStream newFileOutputStream(final File file) throws IOException {
            return targetOut.asNonClosing();
        }

        @Override
        protected Integer newFixedLength() {
            return parent.getTable().getValueFixedLength();
        }

    }

    public static <K, V> void doUpdate(final ITimeSeriesUpdaterInternalMethods<K, V> parent,
            final long initialPrecedingMemoryOffset, final long initialMemoryOffset,
            final long initialPrecedingValueCount, final ICloseableIterable<? extends V> source) {

        final File tempDir = new File(
                parent.getLookupTable().getDirectoryVersionData().getDirectoryVersionDataPerNode(),
                ATimeSeriesUpdater.class.getSimpleName());
        Files.deleteQuietly(tempDir);
        try {
            Files.forceMkdir(tempDir);
        } catch (final IOException e1) {
            throw new RuntimeException(e1);
        }

        try (ICloseableIterator<SequentialChunkedUpdateProgress<K, V>> batchWriterProducer = new ICloseableIterator<SequentialChunkedUpdateProgress<K, V>>() {

            private final SequentialChunkedUpdateProgress<K, V> progress = new SequentialChunkedUpdateProgress<K, V>(
                    parent, initialPrecedingMemoryOffset, initialMemoryOffset, initialPrecedingValueCount, tempDir);
            private final ICloseableIterator<? extends V> elements = source.iterator();

            @Override
            public boolean hasNext() {
                return elements.hasNext();
            }

            @Override
            public SequentialChunkedUpdateProgress<K, V> next() {
                progress.reset();
                try {
                    while (true) {
                        final V element = elements.next();
                        final FDate startTime = parent.extractStartTime(element);
                        final FDate endTime = parent.extractEndTime(element);
                        if (progress.onElement(element, startTime, endTime)) {
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
            flush(parent, batchWriterProducer);
            if (batchWriterProducer.hasNext()) {
                throw new IllegalStateException(
                        "there are still elements to be processed, but the parallel producer did not feed them");
            }
        }
        //clean up temp files (now that memory file out channel has been synced with filesystem)
        Files.deleteQuietly(tempDir);
    }

    private static <K, V> void flush(final ITimeSeriesUpdaterInternalMethods<K, V> parent,
            final ICloseableIterator<SequentialChunkedUpdateProgress<K, V>> batchWriterProducer) {
        int flushIndex = 0;
        try {
            while (true) {
                final SequentialChunkedUpdateProgress<K, V> progress = batchWriterProducer.next();
                final boolean complete = !parent.shouldRedoLastFile()
                        || (progress.getValueCount() == parent.getLookupTable().getBatchFlushInterval()
                                && batchWriterProducer.hasNext());
                progress.write(flushIndex++, complete);
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
    }

}