package de.invesdwin.context.persistence.timeseries.timeseriesdb;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.lang.description.TextDescription;
import de.invesdwin.util.streams.pool.PooledFastByteArrayOutputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;

@NotThreadSafe
public class HeapSerializingCollection<E> extends SerializingCollection<E> {

    private final byte[] bytes;

    public HeapSerializingCollection(final TextDescription name) {
        super(name, null, false);
        getFinalizer().bos = PooledFastByteArrayOutputStream.newInstance();
        this.bytes = null;
    }

    public HeapSerializingCollection(final TextDescription name, final byte[] bytes) {
        super(name, null, true);
        getFinalizer().bos = null;
        this.bytes = bytes;
    }

    @Override
    protected HeapSerializingCollectionFinalizer getFinalizer() {
        return (HeapSerializingCollectionFinalizer) super.getFinalizer();
    }

    @Override
    protected SerializingCollectionFinalizer newFinalizer() {
        return new HeapSerializingCollectionFinalizer();
    }

    @Override
    protected InputStream newFileInputStream(final File file) throws IOException {
        if (bytes != null) {
            return new FastByteArrayInputStream(bytes);
        } else {
            return new FastByteArrayInputStream(getBytes());
        }
    }

    @SuppressWarnings("resource")
    public byte[] getBytes() {
        if (bytes != null) {
            return bytes;
        } else {
            flush();
            return getFinalizer().bos.toByteArray();
        }
    }

    @Override
    protected OutputStream newFileOutputStream(final File file) throws IOException {
        final HeapSerializingCollectionFinalizer finalizer = getFinalizer();
        if (finalizer.bos != null) {
            return finalizer.bos;
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public byte[] getBytesAndClose() {
        try {
            return getBytes();
        } finally {
            super.close();
        }
    }

    protected static class HeapSerializingCollectionFinalizer extends SerializingCollectionFinalizer {

        private PooledFastByteArrayOutputStream bos;

        @Override
        protected void clean() {
            super.clean();
            if (bos != null) {
                bos.close();
                bos = null;
            }
        }

    }

}
