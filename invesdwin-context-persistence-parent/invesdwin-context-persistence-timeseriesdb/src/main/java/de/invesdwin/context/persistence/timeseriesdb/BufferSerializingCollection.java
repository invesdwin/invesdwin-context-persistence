package de.invesdwin.context.persistence.timeseriesdb;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.filechannel.IFileChannel;
import de.invesdwin.norva.beanpath.IntCountingOutputStream;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@NotThreadSafe
public class BufferSerializingCollection<E> extends SerializingCollection<E> {

    private final IByteBuffer buffer;
    private final IntCountingOutputStream out;

    public BufferSerializingCollection(final TextDescription name, final IByteBuffer buffer, final boolean readOnly) {
        super(name, null, false);
        this.buffer = buffer;
        if (readOnly) {
            this.out = null;
        } else {
            this.out = new IntCountingOutputStream(buffer.asOutputStream());
        }
    }

    @Override
    protected InputStream newFileInputStream(final IFileChannel file) throws IOException {
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
    protected OutputStream newFileOutputStream(final IFileChannel file) throws IOException {
        if (out != null) {
            return out;
        } else {
            throw new UnsupportedOperationException();
        }
    }

}
