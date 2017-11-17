package de.invesdwin.context.persistence.leveldb.ipc.mapped;

import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.instrument.DynamicInstrumentationReflections;

/**
 * Class for direct access to a memory mapped file.
 * 
 * This class was inspired from an entry in Bryce Nyeggen's blog
 *
 * https://github.com/caplogic/Mappedbus/blob/master/src/main/io/mappedbus/MemoryMappedFile.java
 *
 */
@SuppressWarnings("restriction")
@NotThreadSafe
public class MemoryMappedFile {

    private static final sun.misc.Unsafe UNSAFE = DynamicInstrumentationReflections.getUnsafe();
    private static final Method MMAP;
    private static final Method UNMMAP;
    private static final int BYTE_ARRAY_OFFSET;

    private long addr;
    private final long size;
    private final String loc;

    static {
        try {
            MMAP = getMethod(sun.nio.ch.FileChannelImpl.class, "map0", int.class, long.class, long.class);
            UNMMAP = getMethod(sun.nio.ch.FileChannelImpl.class, "unmap0", long.class, long.class);
            BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Constructs a new memory mapped file.
     * 
     * @param loc
     *            the file name
     * @param len
     *            the file length
     * @throws Exception
     *             in case there was an error creating the memory mapped file
     */
    public MemoryMappedFile(final String loc, final long len) throws Exception {
        this.loc = loc;
        this.size = roundTo4096(len);
        mapAndSetOffset();
    }

    private static Method getMethod(final Class<?> cls, final String name, final Class<?>... params) throws Exception {
        final Method m = cls.getDeclaredMethod(name, params);
        m.setAccessible(true);
        return m;
    }

    private static long roundTo4096(final long i) {
        return (i + 0xfffL) & ~0xfffL;
    }

    private void mapAndSetOffset() throws Exception {
        final RandomAccessFile backingFile = new RandomAccessFile(this.loc, "rw");
        backingFile.setLength(this.size);
        final FileChannel ch = backingFile.getChannel();
        this.addr = (long) MMAP.invoke(ch, 1, 0L, this.size);
        ch.close();
        backingFile.close();
    }

    public void unmap() throws Exception {
        UNMMAP.invoke(null, addr, this.size);
    }

    /**
     * Reads a byte from the specified position.
     * 
     * @param pos
     *            the position in the memory mapped file
     * @return the value read
     */
    public byte getByte(final long pos) {
        return UNSAFE.getByte(pos + addr);
    }

    /**
     * Reads a byte (volatile) from the specified position.
     * 
     * @param pos
     *            the position in the memory mapped file
     * @return the value read
     */
    public byte getByteVolatile(final long pos) {
        return UNSAFE.getByteVolatile(null, pos + addr);
    }

    /**
     * Reads an int from the specified position.
     * 
     * @param pos
     *            the position in the memory mapped file
     * @return the value read
     */
    public int getInt(final long pos) {
        return UNSAFE.getInt(pos + addr);
    }

    /**
     * Reads an int (volatile) from the specified position.
     * 
     * @param pos
     *            position in the memory mapped file
     * @return the value read
     */
    public int getIntVolatile(final long pos) {
        return UNSAFE.getIntVolatile(null, pos + addr);
    }

    /**
     * Reads a long from the specified position.
     * 
     * @param pos
     *            position in the memory mapped file
     * @return the value read
     */
    public long getLong(final long pos) {
        return UNSAFE.getLong(pos + addr);
    }

    /**
     * Reads a long (volatile) from the specified position.
     * 
     * @param pos
     *            position in the memory mapped file
     * @return the value read
     */
    public long getLongVolatile(final long pos) {
        return UNSAFE.getLongVolatile(null, pos + addr);
    }

    /**
     * Writes a byte to the specified position.
     * 
     * @param pos
     *            the position in the memory mapped file
     * @param val
     *            the value to write
     */
    public void putByte(final long pos, final byte val) {
        UNSAFE.putByte(pos + addr, val);
    }

    /**
     * Writes a byte (volatile) to the specified position.
     * 
     * @param pos
     *            the position in the memory mapped file
     * @param val
     *            the value to write
     */
    public void putByteVolatile(final long pos, final byte val) {
        UNSAFE.putByteVolatile(null, pos + addr, val);
    }

    /**
     * Writes an int to the specified position.
     * 
     * @param pos
     *            the position in the memory mapped file
     * @param val
     *            the value to write
     */
    public void putInt(final long pos, final int val) {
        UNSAFE.putInt(pos + addr, val);
    }

    /**
     * Writes an int (volatile) to the specified position.
     * 
     * @param pos
     *            the position in the memory mapped file
     * @param val
     *            the value to write
     */
    public void putIntVolatile(final long pos, final int val) {
        UNSAFE.putIntVolatile(null, pos + addr, val);
    }

    /**
     * Writes a long to the specified position.
     * 
     * @param pos
     *            the position in the memory mapped file
     * @param val
     *            the value to write
     */
    public void putLong(final long pos, final long val) {
        UNSAFE.putLong(pos + addr, val);
    }

    /**
     * Writes a long (volatile) to the specified position.
     * 
     * @param pos
     *            the position in the memory mapped file
     * @param val
     *            the value to write
     */
    public void putLongVolatile(final long pos, final long val) {
        UNSAFE.putLongVolatile(null, pos + addr, val);
    }

    /**
     * Reads a buffer of data.
     * 
     * @param pos
     *            the position in the memory mapped file
     * @param data
     *            the input buffer
     * @param offset
     *            the offset in the buffer of the first byte to read data into
     * @param length
     *            the length of the data
     */
    public void getBytes(final long pos, final byte[] data, final int offset, final int length) {
        UNSAFE.copyMemory(null, pos + addr, data, BYTE_ARRAY_OFFSET + offset, length);
    }

    /**
     * Writes a buffer of data.
     * 
     * @param pos
     *            the position in the memory mapped file
     * @param data
     *            the output buffer
     * @param offset
     *            the offset in the buffer of the first byte to write
     * @param length
     *            the length of the data
     */
    public void setBytes(final long pos, final byte[] data, final int offset, final int length) {
        UNSAFE.copyMemory(data, BYTE_ARRAY_OFFSET + offset, null, pos + addr, length);
    }

    public boolean compareAndSwapInt(final long pos, final int expected, final int value) {
        return UNSAFE.compareAndSwapInt(null, pos + addr, expected, value);
    }

    public boolean compareAndSwapLong(final long pos, final long expected, final long value) {
        return UNSAFE.compareAndSwapLong(null, pos + addr, expected, value);
    }

    public long getAndAddLong(final long pos, final long delta) {
        return UNSAFE.getAndAddLong(null, pos + addr, delta);
    }
}