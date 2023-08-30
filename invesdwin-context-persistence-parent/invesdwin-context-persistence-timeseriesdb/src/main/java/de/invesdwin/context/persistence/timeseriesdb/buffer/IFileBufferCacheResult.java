package de.invesdwin.context.persistence.timeseriesdb.buffer;

import java.io.Closeable;
import java.util.List;
import java.util.function.Function;

import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.IReverseCloseableIterable;
import de.invesdwin.util.time.date.FDate;

public interface IFileBufferCacheResult<V> extends IReverseCloseableIterable<V>, Closeable {

    V getLatestValue(Function<V, FDate> extractEndTime, FDate key);

    V getLatestValue(int index);

    int getLatestValueIndex(Function<V, FDate> extractEndTime, FDate key);

    ICloseableIterator<V> iterator(Function<V, FDate> extractEndTime, FDate from, FDate to);

    ICloseableIterator<V> reverseIterator(Function<V, FDate> extractEndTime, FDate from, FDate to);

    default ICloseableIterable<V> iterable(final Function<V, FDate> extractEndTime, final FDate from, final FDate to) {
        return new ICloseableIterable<V>() {
            @Override
            public ICloseableIterator<V> iterator() {
                return IFileBufferCacheResult.this.iterator(extractEndTime, from, to);
            }
        };
    }

    default ICloseableIterable<V> reverseIterable(final Function<V, FDate> extractEndTime, final FDate from,
            final FDate to) {
        return new ICloseableIterable<V>() {
            @Override
            public ICloseableIterator<V> iterator() {
                return IFileBufferCacheResult.this.reverseIterator(extractEndTime, from, to);
            }
        };
    }

    @Override
    void close();

    void addToList(List<V> toList);

}
