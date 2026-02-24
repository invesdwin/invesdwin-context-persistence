package de.invesdwin.context.persistence.timeseriesdb.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.function.Function;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.compress.utils.IOUtils;

import de.invesdwin.context.persistence.timeseriesdb.IDeserializingCloseableIterable;
import de.invesdwin.context.system.array.IPrimitiveArrayAllocator;
import de.invesdwin.norva.beanpath.IntCountingOutputStream;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.bytebuffer.AByteBufferCloseableIterable;
import de.invesdwin.util.collections.iterable.bytebuffer.ByteBufferList;
import de.invesdwin.util.collections.iterable.collection.ListCloseableIterable;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.marshallers.serde.FlyweightSerdeProviders;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBuffer;
import de.invesdwin.util.time.date.BisectDuplicateKeyHandling;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;

@ThreadSafe
public class ByteBufferFileBufferCacheResult<V> extends AByteBufferCloseableIterable<V>
        implements IFileBufferCacheResult<V> {

    private final IByteBuffer buffer;
    private final ISerde<V> serde;
    private final int fixedLength;
    private final List<V> list;
    private final ListCloseableIterable<V> delegate;

    public ByteBufferFileBufferCacheResult(final IPrimitiveArrayAllocator arrayAllocator,
            final IDeserializingCloseableIterable<V> delegate) {
        final String name = delegate.getName();
        final IByteBuffer bufferCached = arrayAllocator.getByteBuffer(name);
        if (bufferCached != null) {
            this.buffer = bufferCached;
        } else {
            final ICloseableByteBuffer pooledBuffer;
            if (arrayAllocator.isOnHeap(delegate.size())) {
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
        this.serde = FlyweightSerdeProviders.extractSerde(delegate.getSerde());
        this.fixedLength = delegate.getFixedLength();
        Assertions.checkTrue(fixedLength > 0);
        this.list = new ByteBufferList<>(buffer, serde, fixedLength);
        this.delegate = new ListCloseableIterable<>(list);
    }

    public ByteBufferFileBufferCacheResult(final IByteBuffer buffer, final ISerde<V> serde, final Integer fixedLength) {
        this.buffer = buffer;
        this.serde = FlyweightSerdeProviders.extractSerde(serde);
        this.fixedLength = fixedLength;
        Assertions.checkTrue(fixedLength > 0);
        this.list = new ByteBufferList<>(buffer, serde, fixedLength);
        this.delegate = new ListCloseableIterable<>(list);
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
    public ICloseableIterator<V> iterator(final Function<V, FDate> extractEndTime, final FDate low, final FDate high) {
        if (list.isEmpty()) {
            return EmptyCloseableIterator.getInstance();
        }
        final int lowIndex = determineLowIndex(extractEndTime, low);
        final int lastIndex = list.size() - 1;
        if (lowIndex > lastIndex) {
            return EmptyCloseableIterator.getInstance();
        }
        final int highIndex = determineHighIndex(extractEndTime, high, lastIndex);
        if (highIndex < 0) {
            return EmptyCloseableIterator.getInstance();
        }
        if (lowIndex > highIndex) {
            return EmptyCloseableIterator.getInstance();
        }
        if (lowIndex == highIndex) {
            if (low != null) {
                final FDate lowIndexTime = extractEndTime.apply(list.get(lowIndex));
                if (lowIndexTime.isBeforeNotNullSafe(low)) {
                    return EmptyCloseableIterator.getInstance();
                }
            }
            if (high != null) {
                final FDate highIndexTime = extractEndTime.apply(list.get(highIndex));
                if (highIndexTime.isAfterNotNullSafe(high)) {
                    return EmptyCloseableIterator.getInstance();
                }
            }
        }
        final ICloseableIterator<V> delegate = this.delegate.iterator(lowIndex, highIndex);
        return delegate;
    }

    @Override
    public ICloseableIterator<V> reverseIterator(final Function<V, FDate> extractEndTime, final FDate high,
            final FDate low) {
        if (list.isEmpty()) {
            return EmptyCloseableIterator.getInstance();
        }
        final int lowIndex = determineLowIndex(extractEndTime, low);
        final int lastIndex = list.size() - 1;
        if (lowIndex > lastIndex) {
            return EmptyCloseableIterator.getInstance();
        }
        final int highIndex = determineHighIndex(extractEndTime, high, lastIndex);
        if (highIndex < 0) {
            return EmptyCloseableIterator.getInstance();
        }
        if (lowIndex > highIndex) {
            return EmptyCloseableIterator.getInstance();
        }
        if (lowIndex == highIndex) {
            if (low != null) {
                final FDate lowIndexTime = extractEndTime.apply(list.get(lowIndex));
                if (lowIndexTime.isBeforeNotNullSafe(low)) {
                    return EmptyCloseableIterator.getInstance();
                }
            }
            if (high != null) {
                final FDate highIndexTime = extractEndTime.apply(list.get(highIndex));
                if (highIndexTime.isAfterNotNullSafe(high)) {
                    return EmptyCloseableIterator.getInstance();
                }
            }
        }
        final ICloseableIterator<V> delegate = this.delegate.reverseIterator(highIndex, lowIndex);
        return delegate;
    }

    @Override
    public V getLatestValue(final Function<V, FDate> extractEndTime, final FDate key) {
        final int lastIndex = list.size() - 1;
        final int highIndex = determineHighIndex(extractEndTime, key, lastIndex);
        if (highIndex < 0) {
            return null;
        }
        return list.get(highIndex);
    }

    @Override
    public int getLatestValueIndex(final Function<V, FDate> extractEndTime, final FDate key) {
        final int lastIndex = list.size() - 1;
        final int highIndex = determineHighIndex(extractEndTime, key, lastIndex);
        if (highIndex < 0) {
            return -1;
        }
        return highIndex;
    }

    @Override
    public V getLatestValue(final int index) {
        if (index < 0) {
            return null;
        }
        if (index >= list.size()) {
            return null;
        }
        return list.get(index);
    }

    private int determineLowIndex(final Function<V, FDate> extractEndTime, final FDate low) {
        final int lowIndex;
        if (low == null || low.isBeforeNotNullSafe(extractEndTime.apply(list.get(0)))) {
            lowIndex = 0;
        } else {
            final int potentialLowIndex = FDates.bisect(extractEndTime, list, low, BisectDuplicateKeyHandling.LOWEST);
            final FDate potentialLowTime = extractEndTime.apply(list.get(potentialLowIndex));
            if (potentialLowTime.isBeforeNotNullSafe(low)) {
                lowIndex = potentialLowIndex + 1;
            } else {
                lowIndex = potentialLowIndex;
            }
        }
        return lowIndex;
    }

    private int determineHighIndex(final Function<V, FDate> extractEndTime, final FDate high, final int lastIndex) {
        if (list.isEmpty()) {
            return -1;
        }
        final int highIndex;
        if (high == null || high.isAfterNotNullSafe(extractEndTime.apply(list.get(lastIndex)))) {
            highIndex = lastIndex;
        } else {
            final int potentialHighIndex = FDates.bisect(extractEndTime, list, high,
                    BisectDuplicateKeyHandling.HIGHEST);
            final FDate potentialHighTime = extractEndTime.apply(list.get(potentialHighIndex));
            if (potentialHighTime.isAfterNotNullSafe(high)) {
                highIndex = potentialHighIndex - 1;
            } else {
                highIndex = potentialHighIndex;
            }
        }
        return highIndex;
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
