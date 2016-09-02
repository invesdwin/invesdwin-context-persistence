
package de.invesdwin.context.persistence.leveldb.ipc.mapped;

import javax.annotation.concurrent.NotThreadSafe;

import io.mappedbus.MemoryMappedFile;

@NotThreadSafe
public class ExtendedMemoryMappedFile extends MemoryMappedFile {

    protected ExtendedMemoryMappedFile(final String loc, final long len) throws Exception {
        super(loc, len);
    }

    @Override
    public void unmap() throws Exception {
        super.unmap();
    }

    @Override
    public void putByteVolatile(final long pos, final byte val) {
        super.putByteVolatile(pos, val);
    }

    @Override
    public void putIntVolatile(final long pos, final int val) {
        super.putIntVolatile(pos, val);
    }

    @Override
    public void putLongVolatile(final long pos, final long val) {
        super.putLongVolatile(pos, val);
    }

    @Override
    public boolean compareAndSwapInt(final long pos, final int expected, final int value) {
        return super.compareAndSwapInt(pos, expected, value);
    }

    @Override
    public boolean compareAndSwapLong(final long pos, final long expected, final long value) {
        return super.compareAndSwapLong(pos, expected, value);
    }

}