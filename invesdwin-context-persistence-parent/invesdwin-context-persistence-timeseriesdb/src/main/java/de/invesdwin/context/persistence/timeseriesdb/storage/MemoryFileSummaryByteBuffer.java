package de.invesdwin.context.persistence.timeseriesdb.storage;

import java.io.Closeable;

import javax.annotation.concurrent.ThreadSafe;

import org.agrona.MutableDirectBuffer;

import de.invesdwin.util.math.Integers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.delegate.ADelegateByteBuffer;
import de.invesdwin.util.streams.buffer.file.IMemoryMappedFile;
import de.invesdwin.util.streams.buffer.memory.IMemoryBuffer;

@ThreadSafe
public class MemoryFileSummaryByteBuffer extends ADelegateByteBuffer implements Closeable {

    private final MemoryFileSummary summary;
    //keep reference to prevent finalizer from running too early
    @SuppressWarnings("unused")
    private volatile IMemoryMappedFile file;
    private volatile IByteBuffer buffer;

    public MemoryFileSummaryByteBuffer(final MemoryFileSummary summary) {
        this.summary = summary;
    }

    @Override
    public IByteBuffer getDelegate() {
        return this.buffer;
    }

    public void init(final IMemoryMappedFile file) {
        final int length = Integers.checkedCast(summary.getMemoryLength());
        this.buffer = file.newByteBuffer(summary.getMemoryOffset(), length);
        this.file = file;
    }

    @Override
    public MutableDirectBuffer asDirectBuffer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableDirectBuffer asDirectBuffer(final int index, final int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableDirectBuffer asDirectBufferFrom(final int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableDirectBuffer asDirectBufferTo(final int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IMemoryBuffer asMemoryBuffer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IMemoryBuffer asMemoryBuffer(final int index, final int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IMemoryBuffer asMemoryBufferFrom(final int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IMemoryBuffer asMemoryBufferTo(final int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public java.nio.ByteBuffer asNioByteBuffer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public java.nio.ByteBuffer asNioByteBuffer(final int index, final int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public java.nio.ByteBuffer asNioByteBufferFrom(final int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public java.nio.ByteBuffer asNioByteBufferTo(final int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public java.nio.ByteBuffer nioByteBuffer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableDirectBuffer directBuffer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        this.buffer = ClosedByteBuffer.INSTANCE;
        this.file = null;
    }

}
