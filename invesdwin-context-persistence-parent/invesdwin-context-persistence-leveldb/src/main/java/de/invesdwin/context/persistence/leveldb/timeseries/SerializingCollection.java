package de.invesdwin.context.persistence.leveldb.timeseries;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SerializationException;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.collections.iterable.ACloseableIterator;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.IReverseCloseableIterable;
import de.invesdwin.util.collections.iterable.LimitingIterator;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.UniqueNameGenerator;
import de.invesdwin.util.math.decimal.Decimal;
import de.invesdwin.util.math.decimal.scaled.ByteSize;
import de.invesdwin.util.math.decimal.scaled.ByteSizeScale;
import ezdb.serde.Serde;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;

@NotThreadSafe
public class SerializingCollection<E> implements Collection<E>, IReverseCloseableIterable<E>, Serializable, Closeable {

    public static final int LARGE_BLOCK_SIZE = new ByteSize(new Decimal("1"), ByteSizeScale.MEGABYTES)
            .getValue(ByteSizeScale.BYTES)
            .intValue();
    /*
     * 64KB is default in LZ4OutputStream (1 << 16) though 128K is almost the same speed with a bit better compression
     * on fast compressor
     * 
     * http://java-performance.info/performance-general-compression/
     */
    public static final int DEFAULT_BLOCK_SIZE = new ByteSize(new Decimal("64"), ByteSizeScale.KILOBYTES)
            .getValue(ByteSizeScale.BYTES)
            .intValue();
    public static final int DEFAULT_SEED = 0x9747b28c;
    private static final int READ_ONLY_FILE_SIZE = Integer.MAX_VALUE;
    private static final UniqueNameGenerator UNIQUE_NAME_GENERATOR = new UniqueNameGenerator() {
        @Override
        protected long getInitialValue() {
            return 1;
        }
    };

    private static final byte[] ELEMENT_DELIMITER = "\n".getBytes();
    private int size;
    private final File file;
    private OutputStream fos;
    private boolean closed;
    private boolean firstElement = true;
    private final Integer fixedLength = getFixedLength();
    private final Serde<E> serde = newSerde();

    public SerializingCollection(final String id) {
        this.file = new File(getTempFolder(), UNIQUE_NAME_GENERATOR.get(id.replace(":", "_")) + ".data");
        if (file.exists()) {
            throw new IllegalStateException("File [" + file.getAbsolutePath() + "] already exists!");
        }
    }

    public SerializingCollection(final File file, final boolean readOnly) {
        this.file = file;
        if (readOnly) {
            //allow deserializing only if file contains data already
            this.size = READ_ONLY_FILE_SIZE;
            this.closed = true;
        }
    }

    public File getFile() {
        return file;
    }

    private File getTempFolder() {
        final File tempFolder = new File(ContextProperties.TEMP_DIRECTORY, SerializingCollection.class.getSimpleName());
        try {
            FileUtils.forceMkdir(tempFolder);
        } catch (final IOException e) {
            throw Err.process(e);
        }
        return tempFolder;
    }

    private OutputStream getFos() {
        if (fos == null) {
            //Lazy init to prevent too many open files Exceptions
            if (closed) {
                throw new IllegalStateException("false expected");
            }
            try {
                fos = newCompressor(newFileOutputStream(file));
            } catch (final IOException e) {
                throw Err.process(e);
            }
        }
        return fos;
    }

    @Override
    public boolean add(final E element) {
        if (this.size == READ_ONLY_FILE_SIZE) {
            throw new IllegalStateException(
                    "File [" + file + "] is in read only mode since it contained data when it was opened!");
        }
        try {
            final byte[] bytes = serde.toBytes(element);
            if (bytes == null || bytes.length == 0) {
                throw new IllegalStateException("bytes should contain actual data: " + element);
            }
            if (fixedLength == null) {
                final byte[] encoded = Base64.encodeBase64(bytes);
                if (encoded.length == 0) {
                    throw new IllegalStateException(
                            "empty encoded bytes should not occur since empty bytes should aready be filtered");
                }
                if (firstElement) {
                    firstElement = false;
                } else {
                    getFos().write(ELEMENT_DELIMITER);
                }
                getFos().write(encoded);
            } else {
                if (bytes.length != fixedLength) {
                    throw new IllegalArgumentException(
                            "Serialized object [" + element + "] has unexpected byte length of [" + bytes.length
                                    + "] while fixed length [" + fixedLength + "] was expected!");
                }
                getFos().write(bytes);
            }

        } catch (final IOException e) {
            throw Err.process(e);
        }
        size++;
        return true;
    }

    protected OutputStream newCompressor(final OutputStream out) {
        //LZ4HC is read optimized, you can write optimize by using fastCompressor()
        return new BufferedOutputStream(newDefaultLZ4BlockOutputStream(out), DEFAULT_BLOCK_SIZE);
    }

    protected InputStream newDecompressor(final InputStream inputStream) {
        return newDefaultLZ4BlockInputStream(inputStream);
    }

    public static LZ4BlockOutputStream newDefaultLZ4BlockOutputStream(final OutputStream out) {
        return newLZ4BlockOutputStream(out, DEFAULT_BLOCK_SIZE);
    }

    public static LZ4BlockOutputStream newLargeLZ4BlockOutputStream(final OutputStream out) {
        return newLZ4BlockOutputStream(out, LARGE_BLOCK_SIZE);
    }

    public static LZ4BlockOutputStream newLZ4BlockOutputStream(final OutputStream out, final int blockSize) {
        return new LZ4BlockOutputStream(out, blockSize, LZ4Factory.fastestInstance().fastCompressor(),
                XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED).asChecksum(), true);
    }

    public static LZ4BlockInputStream newDefaultLZ4BlockInputStream(final InputStream in) {
        return new LZ4BlockInputStream(in, LZ4Factory.fastestInstance().fastDecompressor());
    }

    protected Serde<E> newSerde() {
        return new Serde<E>() {
            @Override
            public E fromBytes(final byte[] bytes) {
                return Objects.deserialize(bytes);
            }

            @Override
            public byte[] toBytes(final E obj) {
                return Objects.serialize((Serializable) obj);
            }
        };
    }

    /**
     * Override this to define a fixed length format and thus skip base64 encoding for better performance (though this
     * might behave badly for lists and other dynamic stuff; assertions will tell you someting is wrong
     */
    protected Integer getFixedLength() {
        return null;
    }

    /**
     * Closes this Iterable for more add() operations.
     */
    @Override
    public void close() {
        if (!closed) {
            IOUtils.closeQuietly(fos);
            fos = null;
            closed = true;
        }
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void clear() {
        if (file != null) {
            file.delete();
        }
    }

    @Override
    public ICloseableIterator<E> iterator() {
        if (size() > 0) {
            if (closed) {
                return newIterator();
            } else {
                try {
                    //need to flush contents so we can actually read them
                    fos.flush();
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
                //we allow iteration up to the current size
                return new LimitingIterator<E>(newIterator(), size());
            }
        } else {
            return EmptyCloseableIterator.getInstance();
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
            iterator = new FixedLengthDeserializingIterator();
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
        return size == 0;
    }

    @Override
    public boolean contains(final Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        final List<E> list = new ArrayList<E>();
        for (final E e : this) {
            list.add(e);
        }
        return list.toArray();
    }

    @Override
    public <T> T[] toArray(final T[] a) {
        final List<E> list = new ArrayList<E>();
        for (final E e : this) {
            list.add(e);
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
        for (final E e : c) {
            if (add(e)) {
                changed = true;
            }
        }
        return changed;
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    protected InputStream newFileInputStream(final File file) throws FileNotFoundException {
        return new BufferedInputStream(new FileInputStream(file));
    }

    protected OutputStream newFileOutputStream(final File file) throws IOException {
        return new FileOutputStream(file);
    }

    @NotThreadSafe
    private class DynamicLengthDeserializingIterator extends ACloseableIterator<E> {

        private BufferedReader lineReader;
        private boolean innerClosed;

        private E cachedElement;

        {
            try {
                lineReader = new BufferedReader(new InputStreamReader(newDecompressor(newFileInputStream(file))));
            } catch (final IOException e) {
                throw Err.process(e);
            }
        }

        @Override
        protected boolean innerHasNext() {
            if (cachedElement != null) {
                return true;
            } else {
                try {
                    cachedElement = readNext();
                    return cachedElement != null;
                } catch (final SerializationException e) {
                    return false;
                }
            }
        }

        @SuppressWarnings({ "null" })
        private E readNext() {
            try {
                if (innerClosed) {
                    return (E) null;
                }
                final String line = lineReader.readLine();
                if (line == null) {
                    innerClose();
                    return (E) null;
                }
                final byte[] bytes = line.getBytes();
                if (bytes.length == 0) {
                    throw new IllegalStateException("empty encoded entries should have been filtered by add()");
                }
                final byte[] decoded = Base64.decodeBase64(bytes);
                if (decoded.length == 0) {
                    throw new IllegalStateException("empty entries should have been filtered by add()");
                }
                return serde.fromBytes(decoded);
            } catch (final IOException e) {
                throw Err.process(e);
            }
        }

        @SuppressWarnings("null")
        @Override
        protected E innerNext() {
            if (cachedElement != null) {
                final E ret = cachedElement;
                cachedElement = (E) null;
                return ret;
            }
            return readNext();
        }

        @Override
        protected void innerClose() {
            if (!innerClosed) {
                innerClosed = true;
                try {
                    lineReader.close();
                    //free memory
                    lineReader = null;
                    cachedElement = null;
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

    }

    @NotThreadSafe
    private class FixedLengthDeserializingIterator extends ACloseableIterator<E> {

        private DataInputStream inputStream;
        private byte[] byteBuffer;
        private boolean innerClosed;

        private E cachedElement;

        {
            try {
                inputStream = new DataInputStream(newDecompressor(newFileInputStream(file)));
                byteBuffer = new byte[fixedLength];
            } catch (final IOException e) {
                throw Err.process(e);
            }
        }

        @Override
        protected boolean innerHasNext() {
            if (cachedElement != null) {
                return true;
            } else {
                try {
                    cachedElement = readNext();
                    return cachedElement != null;
                } catch (final SerializationException e) {
                    return false;
                }
            }
        }

        @SuppressWarnings({ "null" })
        private E readNext() {
            if (innerClosed) {
                return (E) null;
            }
            try {
                inputStream.readFully(byteBuffer);
            } catch (final IOException e) {
                innerClose();
                return null;
            }
            return serde.fromBytes(byteBuffer);
        }

        @SuppressWarnings("null")
        @Override
        protected E innerNext() {
            if (cachedElement != null) {
                final E ret = cachedElement;
                cachedElement = (E) null;
                return ret;
            }
            return readNext();
        }

        @Override
        protected void innerClose() {
            if (!innerClosed) {
                innerClosed = true;
                try {
                    inputStream.close();
                    //free memory
                    inputStream = null;
                    byteBuffer = null;
                    cachedElement = null;
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

    }

    private void writeObject(final java.io.ObjectOutputStream stream) throws IOException {
        if (!closed) {
            throw new IllegalStateException("You need to close this instance before serializing it!");
        }
        stream.defaultWriteObject();
    }

    @Override
    public boolean remove(final Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
        throw new UnsupportedOperationException();
    }

}
