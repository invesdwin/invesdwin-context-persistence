package de.invesdwin.context.persistence.timeseries.timeseriesdb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.lang.description.TextDescription;

@NotThreadSafe
public class HeapSerializingCollection<E> extends SerializingCollection<E> {

    private final ByteArrayOutputStream bos;
    private final byte[] bytes;

    public HeapSerializingCollection(final TextDescription name) {
        super(name, null, false);
        System.out.println("reuse");
        this.bos = new ByteArrayOutputStream();
        this.bytes = null;
    }

    public HeapSerializingCollection(final TextDescription name, final byte[] bytes) {
        super(name, null, true);
        this.bos = null;
        this.bytes = bytes;
    }

    @Override
    protected InputStream newFileInputStream(final File file) throws IOException {
        if (bytes != null) {
            return new ByteArrayInputStream(bytes);
        } else {
            return new ByteArrayInputStream(getBytes());
        }
    }

    public byte[] getBytes() {
        if (bytes != null) {
            return bytes;
        } else {
            flush();
            System.out.println("don't copy");
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
