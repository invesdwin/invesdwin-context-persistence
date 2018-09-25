package de.invesdwin.context.persistence.leveldb.serde;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.io.IOUtils;

import de.invesdwin.context.integration.streams.LZ4Streams;
import de.invesdwin.util.math.Bytes;
import ezdb.serde.Serde;

@Immutable
public class CompressingDelegateSerde<E> implements Serde<E> {

    private final Serde<E> delegate;

    public CompressingDelegateSerde(final Serde<E> delegate) {
        this.delegate = delegate;
    }

    @Override
    public E fromBytes(final byte[] bytes) {
        if (bytes.length == 0) {
            return null;
        }
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
        if (obj == null) {
            return Bytes.EMPTY_ARRAY;
        }
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

    protected OutputStream newCompressor(final OutputStream out) {
        return LZ4Streams.newDefaultLZ4OutputStream(out);
    }

    protected InputStream newDecompressor(final ByteArrayInputStream bis) {
        return LZ4Streams.newDefaultLZ4InputStream(bis);
    }

}
