package de.invesdwin.context.persistence.timeseriesdb.filebuffer;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.collections.iterable.EmptyCloseableIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.collection.ArrayListCloseableIterable;
import de.invesdwin.util.collections.iterable.refcount.RefCountReverseCloseableIterable;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;

@ThreadSafe
public class ArrayFileBufferCacheResult<V> extends RefCountReverseCloseableIterable<V>
        implements IFileBufferCacheResult<V>, Closeable {

    private final ArrayList<V> list;

    public ArrayFileBufferCacheResult(final ArrayList<V> list) {
        super(new ArrayListCloseableIterable<>(list));
        getRefCount().incrementAndGet();
        this.list = list;
    }

    @Override
    public ArrayListCloseableIterable<V> getDelegate() {
        return (ArrayListCloseableIterable<V>) super.getDelegate();
    }

    @Override
    public void close() {
        getRefCount().decrementAndGet();
    }

    public ArrayList<V> getList() {
        return list;
    }

    @Override
    public void addToList(final List<V> toList) {
        toList.addAll(list);
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
        return getDelegate().iterator(lowIndex, highIndex);
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
        return getDelegate().reverseIterator(highIndex, lowIndex);
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

    private int determineLowIndex(final Function<V, FDate> extractEndTime, final FDate low) {
        final int lowIndex;
        if (low == null || low.isBeforeNotNullSafe(extractEndTime.apply(list.get(0)))) {
            lowIndex = 0;
        } else {
            final int potentialLowIndex = FDates.bisect(extractEndTime, list, low);
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
        final int highIndex;
        if (high == null || high.isAfterNotNullSafe(extractEndTime.apply(list.get(lastIndex)))) {
            highIndex = lastIndex;
        } else {
            final int potentialHighIndex = FDates.bisect(extractEndTime, list, high);
            final FDate potentialHighTime = extractEndTime.apply(list.get(potentialHighIndex));
            if (potentialHighTime.isAfterNotNullSafe(high)) {
                highIndex = potentialHighIndex - 1;
            } else {
                highIndex = potentialHighIndex;
            }
        }
        return highIndex;
    }

}
