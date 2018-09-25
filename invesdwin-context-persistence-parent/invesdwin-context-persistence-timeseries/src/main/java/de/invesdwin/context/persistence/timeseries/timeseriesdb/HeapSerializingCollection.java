package de.invesdwin.context.persistence.timeseries.timeseriesdb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class HeapSerializingCollection<E> extends SerializingCollection<E> {

    private final ByteArrayOutputStream bos;
    private final byte[] bytes;

    public HeapSerializingCollection() {
        super(null, false);
        this.bos = new ByteArrayOutputStream();
        this.bytes = null;
    }

    public HeapSerializingCollection(final byte[] bytes) {
        super(null, true);
        this.bos = null;
        this.bytes = bytes;
    }

    @Override
    protected InputStream newFileInputStream(final File file) throws FileNotFoundException {
        if (bytes != null) {
            return new ByteArrayInputStream(bytes);
        } else {
            return new ByteArrayInputStream(bos.toByteArray());
        }
    }

    public byte[] getBytes() {
        if (bytes != null) {
            return bytes;
        } else {
            return bos.toByteArray();
        }
    }

    @Override
    protected OutputStream newFileOutputStream(final File file) throws IOException {
        if (bos != null) {
            return bos;
        } else {
            throw new UnsupportedOperationException();
        }
    }

}
