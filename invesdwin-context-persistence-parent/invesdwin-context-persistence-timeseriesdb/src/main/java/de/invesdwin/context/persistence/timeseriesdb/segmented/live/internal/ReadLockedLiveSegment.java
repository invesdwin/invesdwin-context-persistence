package de.invesdwin.context.persistence.timeseriesdb.segmented.live.internal;

import java.util.concurrent.locks.Lock;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseriesdb.storage.ISkipFileFunction;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.time.date.FDate;

@ThreadSafe
public class ReadLockedLiveSegment<K, V> implements ILiveSegment<K, V> {

    private final ILiveSegment<K, V> delegate;
    private final ILock liveReadLock;

    public ReadLockedLiveSegment(final ILiveSegment<K, V> delegate, final ILock liveReadLock) {
        this.delegate = delegate;
        this.liveReadLock = liveReadLock;
    }

    @Override
    public V getFirstValue() {
        liveReadLock.lock();
        try {
            return delegate.getFirstValue();
        } finally {
            liveReadLock.unlock();
        }
    }

    @Override
    public V getLastValue() {
        liveReadLock.lock();
        try {
            return delegate.getLastValue();
        } finally {
            liveReadLock.unlock();
        }
    }

    @Override
    public SegmentedKey<K> getSegmentedKey() {
        liveReadLock.lock();
        try {
            return delegate.getSegmentedKey();
        } finally {
            liveReadLock.unlock();
        }
    }

    @Override
    public ICloseableIterable<V> rangeValues(final FDate from, final FDate to, final Lock readLock,
            final ISkipFileFunction skipFileFunction) {
        liveReadLock.lock();
        try {
            return delegate.rangeValues(from, to, readLock, skipFileFunction);
        } finally {
            liveReadLock.unlock();
        }
    }

    @Override
    public ICloseableIterable<V> rangeReverseValues(final FDate from, final FDate to, final Lock readLock,
            final ISkipFileFunction skipFileFunction) {
        liveReadLock.lock();
        try {
            return delegate.rangeReverseValues(from, to, readLock, skipFileFunction);
        } finally {
            liveReadLock.unlock();
        }
    }

    /**
     * WARNING: Needs to be write locked from the outside
     */
    @Override
    public void putNextLiveValue(final FDate nextLiveKey, final V nextLiveValue) {
        delegate.putNextLiveValue(nextLiveKey, nextLiveValue);
    }

    @Override
    public V getNextValue(final FDate date, final int shiftForwardUnits) {
        liveReadLock.lock();
        try {
            return delegate.getNextValue(date, shiftForwardUnits);
        } finally {
            liveReadLock.unlock();
        }
    }

    @Override
    public V getLatestValue(final FDate date) {
        liveReadLock.lock();
        try {
            return delegate.getLatestValue(date);
        } finally {
            liveReadLock.unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        liveReadLock.lock();
        try {
            return delegate.isEmpty();
        } finally {
            liveReadLock.unlock();
        }
    }

    /**
     * WARNING: Needs to be write locked from the outside
     */
    @Override
    public void convertLiveSegmentToHistorical() {
        delegate.convertLiveSegmentToHistorical();
    }

    @Override
    public FDate getFirstValueKey() {
        liveReadLock.lock();
        try {
            return delegate.getFirstValueKey();
        } finally {
            liveReadLock.unlock();
        }
    }

    @Override
    public FDate getLastValueKey() {
        liveReadLock.lock();
        try {
            return delegate.getLastValueKey();
        } finally {
            liveReadLock.unlock();
        }
    }

    @Override
    public void close() {
        //don't lock
        delegate.close();
    }

}
