package de.invesdwin.context.persistence.timeseriesdb.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.compress.utils.IOUtils;

import de.invesdwin.context.persistence.timeseriesdb.IDeserializingCloseableIterable;
import de.invesdwin.context.system.array.IPrimitiveArrayAllocator;
import de.invesdwin.context.system.array.OnHeapPrimitiveArrayAllocator;
import de.invesdwin.norva.beanpath.IntCountingOutputStream;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.bytebuffer.AByteBufferCloseableIterable;
import de.invesdwin.util.collections.iterable.skip.ASkippingIterator;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.marshallers.serde.IFlyweightSerdeProvider;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBuffer;
import de.invesdwin.util.time.date.FDate;

@ThreadSafe
public class ArrayAllocatorFileBufferCacheResult<V> extends AByteBufferCloseableIterable<V>
        implements IFileBufferCacheResult<V> {

    private final IByteBuffer buffer;
    private final ISerde<V> serde;
    private final int fixedLength;

    public ArrayAllocatorFileBufferCacheResult(final IPrimitiveArrayAllocator arrayAllocator,
            final IDeserializingCloseableIterable<V> delegate) {
        final String name = delegate.getName();
        final IByteBuffer bufferCached = arrayAllocator.getByteBuffer(name);
        if (bufferCached != null) {
            this.buffer = bufferCached;
        } else {
            final ICloseableByteBuffer pooledBuffer;
            if (arrayAllocator.unwrap(OnHeapPrimitiveArrayAllocator.class) != null) {
                pooledBuffer = ByteBuffers.EXPANDABLE_POOL.borrowObject();
            } else {
                pooledBuffer = ByteBuffers.DIRECT_EXPANDABLE_POOL.borrowObject();
            }
            try {
                final int length;
                try (InputStream in = delegate.newInputStream()) {
                    final IntCountingOutputStream out = new IntCountingOutputStream(pooledBuffer.asOutputStream());
                    IOUtils.copy(in, out);
                    length = out.getCount();
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
                this.buffer = arrayAllocator.newByteBuffer(name, length);
                buffer.putBytes(0, pooledBuffer.slice(0, length));
            } finally {
                pooledBuffer.close();
            }
        }
        this.serde = extractSerde(delegate);
        this.fixedLength = delegate.getFixedLength();
        Assertions.checkTrue(fixedLength > 0);
    }

    @SuppressWarnings("unchecked")
    private ISerde<V> extractSerde(final IDeserializingCloseableIterable<V> delegate) {
        final ISerde<V> serdeProvider = delegate.getSerde();
        if (serdeProvider instanceof IFlyweightSerdeProvider) {
            final IFlyweightSerdeProvider<V> flyweightSerdeProvider = (IFlyweightSerdeProvider<V>) serdeProvider;
            final ISerde<V> flyweightSerde = flyweightSerdeProvider.asFlyweightSerde();
            if (flyweightSerde != null) {
                return flyweightSerde;
            }
        }
        System.err.println("not flyweight: " + serdeProvider.toString());
        return serdeProvider;
    }

    @Override
    protected ISerde<V> getSerde() {
        return serde;
    }

    @Override
    protected int getFixedLength() {
        return fixedLength;
    }

    @Override
    protected IByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public ICloseableIterator<V> iterator(final Function<V, FDate> extractEndTime, final FDate from, final FDate to) {
        if (from == null && to == null) {
            return iterator();
        } else if (from == null) {
            return new ASkippingIterator<V>(iterator()) {
                @Override
                protected boolean skip(final V element) {
                    final FDate time = extractEndTime.apply(element);
                    if (time.isAfterNotNullSafe(to)) {
                        throw FastNoSuchElementException.getInstance("getRangeValues reached end");
                    }
                    return false;
                }
            };
        } else if (to == null) {
            return new ASkippingIterator<V>(iterator()) {
                @Override
                protected boolean skip(final V element) {
                    final FDate time = extractEndTime.apply(element);
                    if (time.isBeforeNotNullSafe(from)) {
                        return true;
                    }
                    return false;
                }
            };
        } else {
            return new ASkippingIterator<V>(iterator()) {
                @Override
                protected boolean skip(final V element) {
                    final FDate time = extractEndTime.apply(element);
                    if (time.isBeforeNotNullSafe(from)) {
                        return true;
                    } else if (time.isAfterNotNullSafe(to)) {
                        throw FastNoSuchElementException.getInstance("getRangeValues reached end");
                    }
                    return false;
                }
            };
        }
    }

    @Override
    public ICloseableIterator<V> reverseIterator(final Function<V, FDate> extractEndTime, final FDate from,
            final FDate to) {
        if (from == null && to == null) {
            return reverseIterator();
        } else if (from == null) {
            return new ASkippingIterator<V>(reverseIterator()) {
                @Override
                protected boolean skip(final V element) {
                    final FDate time = extractEndTime.apply(element);
                    if (time.isBeforeNotNullSafe(to)) {
                        throw FastNoSuchElementException.getInstance("getRangeValues reached end");
                    }
                    return false;
                }
            };
        } else if (to == null) {
            return new ASkippingIterator<V>(reverseIterator()) {
                @Override
                protected boolean skip(final V element) {
                    final FDate time = extractEndTime.apply(element);
                    if (time.isAfterNotNullSafe(from)) {
                        return true;
                    }
                    return false;
                }
            };
        } else {
            return new ASkippingIterator<V>(reverseIterator()) {
                @Override
                protected boolean skip(final V element) {
                    final FDate time = extractEndTime.apply(element);
                    if (time.isAfterNotNullSafe(from)) {
                        return true;
                    } else if (time.isBeforeNotNullSafe(to)) {
                        throw FastNoSuchElementException.getInstance("getRangeValues reached end");
                    }
                    return false;
                }
            };
        }
    }

    @Override
    public V getLatestValue(final Function<V, FDate> extractEndTime, final FDate key) {
        V latestValue = null;
        try (ICloseableIterator<V> it = iterator()) {
            while (true) {
                final V newValue = it.next();
                final FDate newValueTime = extractEndTime.apply(newValue);
                if (newValueTime.isAfter(key)) {
                    break;
                } else {
                    latestValue = newValue;
                }
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
        return latestValue;
    }

    @Override
    public void addToList(final List<V> toList) {
        Lists.toListWithoutHasNext(iterator(), toList);
    }

    @Override
    public void close() {
        //noop
    }

}
