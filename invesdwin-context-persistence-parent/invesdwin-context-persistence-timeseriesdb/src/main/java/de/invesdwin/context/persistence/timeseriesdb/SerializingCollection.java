package de.invesdwin.context.persistence.timeseriesdb;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang3.SerializationException;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.compression.lz4.LZ4Streams;
import de.invesdwin.util.collections.iterable.ACloseableIterator;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.LimitingIterator;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.lang.string.UniqueNameGenerator;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.LocalFastSerializingSerde;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.streams.InputStreams;
import de.invesdwin.util.streams.OutputStreams;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBuffer;

@NotThreadSafe
public class SerializingCollection<E> implements Collection<E>, IDeserializingCloseableIterable<E>, Closeable {

    private static final int READ_ONLY_FILE_SIZE = Integer.MAX_VALUE;
    private static final UniqueNameGenerator UNIQUE_NAME_GENERATOR = new UniqueNameGenerator() {
        @Override
        protected long getInitialValue() {
            return 1;
        }
    };

    private final TextDescription name;
    private int size;
    private boolean empty;
    private final boolean readOnly;
    private final File file;
    private final SerializingCollectionFinalizer finalizer;
    private final Integer fixedLength = newFixedLength();
    @SuppressWarnings("unchecked")
    private final ISerde<E> serde = (ISerde<E>) newSerde();

    public SerializingCollection(final TextDescription name, final String tempFileId) {
        this.name = name;
        this.finalizer = newFinalizer();
        this.file = new File(getTempFolder(),
                Files.normalizePath(UNIQUE_NAME_GENERATOR.get(Files.normalizePath(tempFileId) + ".data")));
        try {
            Files.forceMkdir(file.getParentFile());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        if (file.exists()) {
            throw new IllegalStateException("File [" + file.getAbsolutePath() + "] already exists!");
        }
        this.empty = true;
        this.size = 0;
        this.finalizer.register(this);
        this.readOnly = false;
    }

    public SerializingCollection(final TextDescription name, final File file, final boolean readOnly) {
        this.name = name;
        this.finalizer = newFinalizer();
        this.file = file;
        if (readOnly) {
            //allow deserializing only if file contains data already
            this.size = READ_ONLY_FILE_SIZE;
            this.finalizer.closed = true;
            this.empty = false;
            this.readOnly = true;
        } else {
            this.finalizer.register(this);
            this.empty = file != null && (!file.exists() || file.length() == 0L);
            if (!empty) {
                this.size = READ_ONLY_FILE_SIZE;
            } else {
                this.size = 0;
            }
            this.readOnly = false;
        }
    }

    @Override
    public String getName() {
        return name.toString();
    }

    @Override
    public ISerde<E> getSerde() {
        return serde;
    }

    @Override
    public Integer getFixedLength() {
        return fixedLength;
    }

    @Override
    public final InputStream newInputStream() throws IOException {
        return newDecompressor(newFileInputStream(file));
    }

    protected SerializingCollectionFinalizer newFinalizer() {
        return new SerializingCollectionFinalizer();
    }

    protected SerializingCollectionFinalizer getFinalizer() {
        return finalizer;
    }

    public File getFile() {
        return file;
    }

    private File getTempFolder() {
        return new File(ContextProperties.TEMP_DIRECTORY, SerializingCollection.class.getSimpleName());
    }

    private OutputStream getFos() {
        if (finalizer.fos == null) {
            //Lazy init to prevent too many open files Exceptions
            if (finalizer.closed) {
                throw new IllegalStateException("false expected");
            }
            try {
                finalizer.fos = newCompressor(newFileOutputStream(file));
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
        return finalizer.fos;
    }

    public IByteBuffer getWriteBuffer() {
        if (finalizer.writeBuffer == null) {
            //Lazy init to prevent too many open files Exceptions
            if (finalizer.closed) {
                throw new IllegalStateException("false expected");
            }
            finalizer.writeBuffer = ByteBuffers.EXPANDABLE_POOL.borrowObject();
        }
        return finalizer.writeBuffer;
    }

    @Override
    public boolean add(final E element) {
        if (readOnly) {
            throw new IllegalStateException("File [" + file + "] is in read only mode!");
        }
        try {
            final IByteBuffer writeBuffer = getWriteBuffer();
            final int length = serde.toBuffer(writeBuffer, element);
            if (length == 0) {
                throw new IllegalStateException("bytes should contain actual data: " + element);
            }
            final OutputStream fos = getFos();
            if (fixedLength == null) {
                OutputStreams.writeInt(fos, length);
            } else {
                if (length != fixedLength) {
                    throw new IllegalArgumentException(
                            "Serialized object [" + element + "] has unexpected byte length of [" + length
                                    + "] while fixed length [" + fixedLength + "] was expected!");
                }
            }
            writeBuffer.getBytesTo(0, fos, length);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        size++;
        empty = false;
        return true;
    }

    protected ICompressionFactory getCompressionFactory() {
        return LZ4Streams.getDefaultCompressionFactory();
    }

    protected OutputStream newCompressor(final OutputStream out) {
        //LZ4HC is read optimized, you can write optimize by using fastCompressor()
        return getCompressionFactory().newCompressor(out, false);
    }

    protected InputStream newDecompressor(final InputStream inputStream) {
        return getCompressionFactory().newDecompressor(inputStream);
    }

    protected ISerde<? extends E> newSerde() {
        return LocalFastSerializingSerde.get();
    }

    /**
     * Override this to define a fixed length format and thus skip base64 encoding for better performance (though this
     * might behave badly for lists and other dynamic stuff; assertions will tell you someting is wrong
     */
    protected Integer newFixedLength() {
        return null;
    }

    /**
     * Closes this Iterable for more add() operations.
     */
    @Override
    public void close() {
        finalizer.close();
    }

    /**
     * Ensures that even if nothing has been added to this collection, the file written at least contains an empty
     * array. Thus at least creating an empty file.
     */
    public void closeWithEmptyWrite() {
        if (isEmpty()) {
            try {
                getFos().write(Bytes.EMPTY_ARRAY);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
        close();
    }

    public boolean isClosed() {
        return finalizer.closed;
    }

    @Override
    public void clear() {
        if (file != null) {
            file.delete();
        }
    }

    @Override
    public ICloseableIterator<E> iterator() {
        if (isEmpty()) {
            return EmptyCloseableIterator.getInstance();
        } else {
            if (finalizer.closed) {
                return newIterator();
            } else {
                final OutputStream fosCopy = finalizer.fos;
                if (fosCopy != null) {
                    try {
                        //need to flush contents so we can actually read them
                        fosCopy.flush();
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                //we allow iteration up to the current size
                if (size != READ_ONLY_FILE_SIZE) {
                    return new LimitingIterator<E>(newIterator(), size());
                } else {
                    return newIterator();
                }
            }
        }
    }

    @Override
    public ICloseableIterator<E> reverseIterator() {
        final BufferingIterator<E> reverseIterator = new BufferingIterator<E>();
        try (ICloseableIterator<E> iterator = iterator()) {
            while (true) {
                reverseIterator.prepend(iterator.next());
            }
        } catch (final NoSuchElementException e) {
            //ignore
        }
        return reverseIterator;
    }

    private ICloseableIterator<E> newIterator() {
        final ICloseableIterator<E> iterator;
        if (fixedLength != null) {
            iterator = new FixedLengthDeserializingIterator(fixedLength.intValue());
        } else {
            iterator = new DynamicLengthDeserializingIterator();
        }
        return iterator;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return empty;
    }

    @Override
    public boolean contains(final Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        final List<E> list = new ArrayList<E>();
        try (ICloseableIterator<E> iterator = iterator()) {
            while (true) {
                final E next = iterator.next();
                list.add(next);
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
        return list.toArray();
    }

    @Override
    public <T> T[] toArray(final T[] a) {
        final List<E> list = new ArrayList<E>(Integers.max(10, a.length));
        try (ICloseableIterator<E> iterator = iterator()) {
            while (true) {
                final E next = iterator.next();
                list.add(next);
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
        return list.toArray(a);
    }

    @Override
    public boolean containsAll(final Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(final Collection<? extends E> c) {
        return addAllIterable(c);
    }

    public boolean addAllIterable(final Iterable<? extends E> c) {
        boolean changed = false;

        final Iterator<? extends E> iterator = c.iterator();
        try {
            while (true) {
                final E next = iterator.next();
                if (add(next)) {
                    changed = true;
                }
            }
        } catch (final NoSuchElementException e) {
            //end reached
        } finally {
            Closeables.closeQuietly(iterator);
        }

        return changed;
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    protected InputStream newFileInputStream(final File file) throws IOException {
        return new FileInputStream(file);
    }

    protected OutputStream newFileOutputStream(final File file) throws IOException {
        return new FileOutputStream(file);
    }

    @NotThreadSafe
    private final class DynamicLengthDeserializingIterator extends ACloseableIterator<E> {
        private final DynamicLengthDeserializingIteratorFinalizer<E> finalizer;

        private DynamicLengthDeserializingIterator() {
            super(new TextDescription("%s: %s.%s: %s", name, SerializingCollection.class.getSimpleName(),
                    DynamicLengthDeserializingIteratorFinalizer.class.getSimpleName(), file));
            finalizer = new DynamicLengthDeserializingIteratorFinalizer<>();
            try {
                finalizer.inputStream = newInputStream();
                finalizer.readBuffer = ByteBuffers.EXPANDABLE_POOL.borrowObject();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            finalizer.register(this);
        }

        @Override
        protected boolean innerHasNext() {
            if (finalizer.cachedElement != null) {
                return true;
            } else {
                try {
                    finalizer.cachedElement = readNext();
                    return true;
                } catch (final SerializationException | NoSuchElementException e) {
                    return false;
                }
            }
        }

        private E readNext() {
            try {
                if (finalizer.closed) {
                    throw FastNoSuchElementException
                            .getInstance("SerializingCollection.DynamicLengthDeserializingIterator: readNext() closed");
                }
                final int size;
                try {
                    size = InputStreams.readInt(finalizer.inputStream);
                    finalizer.readBuffer.putBytesTo(0, finalizer.inputStream, size);
                } catch (final EOFException e) {
                    finalizer.close();
                    throw FastNoSuchElementException.getInstance(
                            "SerializingCollection.DynamicLengthDeserializingIterator: readNext() EOFException", e);
                }
                if (size == 0) {
                    throw new IllegalStateException("empty encoded entries should have been filtered by add()");
                }
                final E next = serde.fromBuffer(finalizer.readBuffer.sliceTo(size));
                return next;
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings("null")
        @Override
        protected E innerNext() {
            if (finalizer.cachedElement != null) {
                final E ret = finalizer.cachedElement;
                finalizer.cachedElement = (E) null;
                return ret;
            }
            final E readNext = readNext();
            if (readNext == null) {
                throw new NullPointerException(
                        "SerializingCollection.DynamicLengthDeserializingIterator: readNext() returned null");
            }
            return readNext;
        }

        @Override
        protected void innerClose() {
            finalizer.close();
        }

    }

    private static final class DynamicLengthDeserializingIteratorFinalizer<E> extends AFinalizer {
        private InputStream inputStream;
        private boolean closed;
        private E cachedElement;
        private ICloseableByteBuffer readBuffer;

        @Override
        protected void clean() {
            try {
                inputStream.close();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            //free memory
            inputStream = null;
            cachedElement = null;
            ByteBuffers.EXPANDABLE_POOL.returnObject(readBuffer);
            readBuffer = null;
            closed = true;
        }

        @Override
        protected boolean isCleaned() {
            return closed;
        }

        @Override
        public boolean isThreadLocal() {
            return true;
        }
    }

    @NotThreadSafe
    private final class FixedLengthDeserializingIterator extends ACloseableIterator<E> {

        private final FixedLengthDeserializingIteratorFinalizer<E> finalizer;
        private final int fixedLength;

        private FixedLengthDeserializingIterator(final int fixedLength) {
            super(new TextDescription("%s: %s.%s: %s", name, SerializingCollection.class.getSimpleName(),
                    FixedLengthDeserializingIterator.class.getSimpleName(), file));
            this.fixedLength = fixedLength;
            this.finalizer = new FixedLengthDeserializingIteratorFinalizer<>();
            try {
                this.finalizer.inputStream = newInputStream();
                this.finalizer.readBuffer = ByteBuffers.EXPANDABLE_POOL.borrowObject();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            this.finalizer.register(this);
        }

        @Override
        protected boolean innerHasNext() {
            if (finalizer.cachedElement != null) {
                return true;
            } else {
                try {
                    finalizer.cachedElement = readNext();
                    return true;
                } catch (final SerializationException | NoSuchElementException e) {
                    return false;
                }
            }
        }

        private E readNext() {
            if (finalizer.cleaned) {
                throw FastNoSuchElementException
                        .getInstance("SerializingCollection.FixedLengthDeserializingIterator: readNext() cleaned");
            }
            try {
                finalizer.readBuffer.putBytesTo(0, finalizer.inputStream, fixedLength);
            } catch (final IOException e) {
                //LZ4 can throw ArrayIndexOutOfBounds
                finalizer.close();
                throw FastNoSuchElementException.getInstance(
                        "SerializingCollection.FixedLengthDeserializingIterator: readNext() IOException", e);
            }
            final E next = serde.fromBuffer(finalizer.readBuffer.sliceTo(fixedLength));
            return next;
        }

        @SuppressWarnings("null")
        @Override
        protected E innerNext() {
            if (finalizer.cachedElement != null) {
                final E ret = finalizer.cachedElement;
                finalizer.cachedElement = (E) null;
                return ret;
            }
            final E readNext = readNext();
            if (readNext == null) {
                throw new NullPointerException(
                        "SerializingCollection.FixedLengthDeserializingIterator: readNext() returned null");
            }
            return readNext;
        }

        @Override
        protected void innerClose() {
            finalizer.close();
        }

    }

    private static final class FixedLengthDeserializingIteratorFinalizer<E> extends AFinalizer {
        private InputStream inputStream;
        private ICloseableByteBuffer readBuffer;
        private boolean cleaned;

        private E cachedElement;

        @Override
        protected void clean() {
            try {
                inputStream.close();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            //free memory
            inputStream = null;
            ByteBuffers.EXPANDABLE_POOL.returnObject(readBuffer);
            readBuffer = null;
            cachedElement = null;
            cleaned = true;
        }

        @Override
        protected boolean isCleaned() {
            return cleaned;
        }

        @Override
        public boolean isThreadLocal() {
            return true;
        }
    }

    @Override
    public boolean remove(final Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    public void flush() {
        if (finalizer.fos != null) {
            try {
                finalizer.fos.flush();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected static class SerializingCollectionFinalizer extends AFinalizer {

        private OutputStream fos;
        private boolean closed;
        private ICloseableByteBuffer writeBuffer;

        @Override
        protected void clean() {
            Closeables.closeQuietly(fos);
            fos = null;
            ByteBuffers.EXPANDABLE_POOL.returnObject(writeBuffer);
            writeBuffer = null;
            closed = true;
        }

        @Override
        protected boolean isCleaned() {
            return closed;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }

    }

}
