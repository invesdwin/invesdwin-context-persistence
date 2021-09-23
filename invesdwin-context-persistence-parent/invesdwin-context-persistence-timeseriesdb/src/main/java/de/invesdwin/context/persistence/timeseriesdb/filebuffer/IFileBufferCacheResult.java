package de.invesdwin.context.persistence.timeseriesdb.filebuffer;

import java.io.Closeable;
import java.util.List;
import java.util.function.Function;

import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.IReverseCloseableIterable;
import de.invesdwin.util.time.date.FDate;

public interface IFileBufferCacheResult<V> extends IReverseCloseableIterable<V>, Closeable {

    V getLatestValue(Function<V, FDate> extractEndTime, FDate key);

    ICloseableIterator<V> iterator(Function<V, FDate> extractEndTime, FDate from, FDate to);

    ICloseableIterator<V> reverseIterator(Function<V, FDate> extractEndTime, FDate from, FDate to);

    @Override
    void close();

    void addToList(List<V> toList);

}
