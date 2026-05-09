package de.invesdwin.context.persistence.timeseriesdb.segmented.live.segment;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseriesdb.storage.ISkipFileFunction;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.time.date.FDate;

@ThreadSafe
public class SynchronizedLiveSegment<K, V> implements ILiveSegment<K, V> {

    private final ILiveSegment<K, V> delegate;

    public SynchronizedLiveSegment(final ILiveSegment<K, V> delegate) {
        this.delegate = delegate;
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized <T> T unwrap(final Class<T> type) {
        if (type.isAssignableFrom(getClass())) {
            return (T) this;
        } else {
            return delegate.unwrap(type);
        }
    }

    @Override
    public synchronized V getFirstValue() {
        return delegate.getFirstValue();
    }

    @Override
    public synchronized V getLastValue() {
        return delegate.getLastValue();
    }

    @Override
    public synchronized SegmentedKey<K> getSegmentedKey() {
        return delegate.getSegmentedKey();
    }

    @Override
    public synchronized ICloseableIterable<V> rangeValues(final FDate from, final FDate to, final ILock readLock,
            final ISkipFileFunction skipFileFunction) {
        return delegate.rangeValues(from, to, readLock, skipFileFunction);
    }

    @Override
    public synchronized ICloseableIterable<V> rangeReverseValues(final FDate from, final FDate to, final ILock readLock,
            final ISkipFileFunction skipFileFunction) {
        return delegate.rangeReverseValues(from, to, readLock, skipFileFunction);
    }

    @Override
    public synchronized boolean putNextLiveValue(final FDate nextLiveStartTime, final FDate nextLiveEndTimeKey,
            final V nextLiveValue) {
        return delegate.putNextLiveValue(nextLiveStartTime, nextLiveEndTimeKey, nextLiveValue);
    }

    @Override
    public synchronized V getNextValue(final FDate date, final int shiftForwardUnits) {
        return delegate.getNextValue(date, shiftForwardUnits);
    }

    @Override
    public synchronized V getLatestValue(final FDate date) {
        return delegate.getLatestValue(date);
    }

    @Override
    public synchronized V getLatestValue(final long index) {
        return delegate.getLatestValue(index);
    }

    @Override
    public synchronized long getLatestValueIndex(final FDate date) {
        return delegate.getLatestValueIndex(date);
    }

    @Override
    public synchronized boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public synchronized void convertLiveSegmentToHistorical() {
        delegate.convertLiveSegmentToHistorical();
    }

    @Override
    public synchronized FDate getFirstValueKey() {
        return delegate.getFirstValueKey();
    }

    @Override
    public synchronized FDate getLastValueKey() {
        return delegate.getLastValueKey();
    }

    @Override
    public synchronized long size() {
        return delegate.size();
    }

    @Override
    public synchronized long size(final FDate from, final FDate to) {
        return delegate.size(from, to);
    }

    @Override
    public synchronized void close() {
        delegate.close();
    }

}
