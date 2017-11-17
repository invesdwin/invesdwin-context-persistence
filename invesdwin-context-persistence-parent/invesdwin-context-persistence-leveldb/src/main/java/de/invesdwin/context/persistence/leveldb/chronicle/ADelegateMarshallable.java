package de.invesdwin.context.persistence.leveldb.chronicle;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.IOUtils;

import de.invesdwin.context.persistence.leveldb.timeseries.SerializingCollection;
import de.invesdwin.util.assertions.Assertions;
import ezdb.serde.Serde;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;

/**
 * The marshallable is not allowed to have a reference to a non marshallable object, otherwise chronicle-map will choke
 * on it. Thus we have to wrap the serde in this abomination. Also for funs you have to create these instances in static
 * factories so you don't leak a reference to the enclosing chronicle map.
 */
@ThreadSafe
public abstract class ADelegateMarshallable<T> implements BytesWriter<T>, BytesReader<T> {

    public static final int DEFAULT_COMPRESSION_LEVEL = SerializingCollection.DEFAULT_COMPRESSION_LEVEL;
    public static final int LARGE_BLOCK_SIZE = SerializingCollection.LARGE_BLOCK_SIZE;
    public static final int DEFAULT_BLOCK_SIZE = SerializingCollection.DEFAULT_BLOCK_SIZE;
    public static final int DEFAULT_SEED = SerializingCollection.DEFAULT_SEED;

    @GuardedBy("this")
    private transient Serde<T> serde;

    @SuppressWarnings("rawtypes")
    @Override
    public T read(final net.openhft.chronicle.bytes.Bytes in, final T using) {
        Assertions.checkNull(using);
        final byte[] bytes = in.toByteArray();
        final ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        final InputStream decompressor = newDecompressor(bis);
        try {
            final byte[] decompressedBytes = IOUtils.toByteArray(decompressor);
            return getSerde().fromBytes(decompressedBytes);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(decompressor);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void write(final net.openhft.chronicle.bytes.Bytes out, final T toWrite) {
        final byte[] bytes = getSerde().toBytes(toWrite);
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final OutputStream compressor = newCompressor(bos);
        try {
            IOUtils.write(bytes, compressor);
            compressor.flush();
            final byte[] compressedBytes = bos.toByteArray();
            out.write(compressedBytes);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(compressor);
        }
    }

    private synchronized Serde<T> getSerde() {
        if (serde == null) {
            serde = newSerde();
        }
        return serde;
    }

    protected abstract Serde<T> newSerde();

    protected InputStream newDecompressor(final InputStream in) {
        return SerializingCollection.newDefaultLZ4BlockInputStream(in);
    }

    protected OutputStream newCompressor(final OutputStream out) {
        return newDefaultLZ4BlockOutputStream(out);
    }

    public static LZ4BlockOutputStream newDefaultLZ4BlockOutputStream(final OutputStream out) {
        return newFastLZ4BlockOutputStream(out, DEFAULT_BLOCK_SIZE, DEFAULT_COMPRESSION_LEVEL);
    }

    public static LZ4BlockOutputStream newLargeLZ4BlockOutputStream(final OutputStream out) {
        return newFastLZ4BlockOutputStream(out, LARGE_BLOCK_SIZE, DEFAULT_COMPRESSION_LEVEL);
    }

    public static LZ4BlockOutputStream newFastLZ4BlockOutputStream(final OutputStream out, final int blockSize,
            final int compressionLevel) {
        return new LZ4BlockOutputStream(out, blockSize, LZ4Factory.fastestInstance().fastCompressor(),
                XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED).asChecksum(), true);
    }

}
