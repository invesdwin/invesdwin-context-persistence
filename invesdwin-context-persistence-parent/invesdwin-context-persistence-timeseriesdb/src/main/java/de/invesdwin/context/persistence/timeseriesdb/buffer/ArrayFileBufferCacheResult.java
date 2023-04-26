package de.invesdwin.context.persistence.timeseriesdb.buffer;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.collections.iterable.EmptyCloseableIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.collection.arraylist.ArrayListCloseableIterable;
import de.invesdwin.util.collections.iterable.collection.arraylist.IArrayListCloseableIterable;
import de.invesdwin.util.collections.iterable.collection.arraylist.SynchronizedArrayListCloseableIterable;
import de.invesdwin.util.collections.iterable.refcount.RefCountReverseCloseableIterable;
import de.invesdwin.util.time.date.BisectDuplicateKeyHandling;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;

@ThreadSafe
public class ArrayFileBufferCacheResult<V> extends RefCountReverseCloseableIterable<V>
        implements IFileBufferCacheResult<V>, Closeable {

    private final ArrayList<V> list;

    public ArrayFileBufferCacheResult(final ArrayList<V> list) {
        super(new SynchronizedArrayListCloseableIterable<>(new ArrayListCloseableIterable<>(list)));
        getRefCount().incrementAndGet();
        this.list = list;
    }

    @Override
    public IArrayListCloseableIterable<V> getDelegate() {
        return (IArrayListCloseableIterable<V>) super.getDelegate();
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
        final ICloseableIterator<V> delegate = getDelegate().iterator(lowIndex, highIndex);
        return new ResultReferenceCloseableIterator<V>(this, delegate);
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
        final ICloseableIterator<V> delegate = getDelegate().reverseIterator(highIndex, lowIndex);
        return new ResultReferenceCloseableIterator<V>(this, delegate);
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

    /**
     * Keep the reference to the result so that weak eviction is delayed as long as this is used
     */
    private static final class ResultReferenceCloseableIterator<V> implements ICloseableIterator<V> {
        @SuppressWarnings("unused")
        private final ArrayFileBufferCacheResult<V> result;
        private final ICloseableIterator<V> delegate;

        private ResultReferenceCloseableIterator(final ArrayFileBufferCacheResult<V> result,
                final ICloseableIterator<V> delegate) {
            this.result = result;
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public V next() {
            return delegate.next();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

}
