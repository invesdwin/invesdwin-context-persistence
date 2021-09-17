package de.invesdwin.context.persistence.timeseriesdb;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.norva.beanpath.CountingOutputStream;
import de.invesdwin.util.lang.description.TextDescription;
import de.invesdwin.util.streams.buffer.IByteBuffer;

@NotThreadSafe
public class BufferSerializingCollection<E> extends SerializingCollection<E> {

    private final IByteBuffer buffer;
    private final CountingOutputStream out;

    public BufferSerializingCollection(final TextDescription name, final IByteBuffer buffer, final boolean readOnly) {
        super(name, null, false);
        this.buffer = buffer;
        if (readOnly) {
            this.out = null;
        } else {
            this.out = new CountingOutputStream(buffer.asOutputStream());
        }
    }

    @Override
    protected InputStream newFileInputStream(final File file) throws IOException {
        return buffer.asInputStream();
    }

    public int getBytesCount() {
        if (out == null) {
            return buffer.capacity();
        } else {
            flush();
            return out.getCount();
        }
    }

    public byte[] getBytes() {
        if (out == null) {
            return buffer.asByteArray();
        } else {
            flush();
            return buffer.asByteArrayTo(out.getCount());
        }
    }

    @Override
    protected OutputStream newFileOutputStream(final File file) throws IOException {
        if (out != null) {
            return out;
        } else {
            throw new UnsupportedOperationException();
        }
    }

}
