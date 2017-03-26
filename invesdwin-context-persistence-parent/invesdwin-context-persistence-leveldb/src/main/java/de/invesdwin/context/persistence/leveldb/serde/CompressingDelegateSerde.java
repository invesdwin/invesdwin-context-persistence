package de.invesdwin.context.persistence.leveldb.serde;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.io.IOUtils;

import de.invesdwin.context.persistence.leveldb.timeseries.SerializingCollection;
import ezdb.serde.Serde;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;

@Immutable
public class CompressingDelegateSerde<E> implements Serde<E> {

    private final Serde<E> delegate;

    public CompressingDelegateSerde(final Serde<E> delegate) {
        this.delegate = delegate;
    }

    @Override
    public E fromBytes(final byte[] bytes) {
        try {
            final ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            final InputStream in = newDecompressor(bis);
            final byte[] decompressedBytes = IOUtils.toByteArray(in);
            in.close();
            return delegate.fromBytes(decompressedBytes);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] toBytes(final E obj) {
        try {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final OutputStream out = newCompressor(bos);
            out.write(delegate.toBytes(obj));
            out.close();
            return bos.toByteArray();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected LZ4BlockOutputStream newCompressor(final OutputStream out) {
        return SerializingCollection.newDefaultLZ4BlockOutputStream(out);
    }

    protected LZ4BlockInputStream newDecompressor(final ByteArrayInputStream bis) {
        return new LZ4BlockInputStream(bis, LZ4Factory.fastestInstance().fastDecompressor());
    }

}
