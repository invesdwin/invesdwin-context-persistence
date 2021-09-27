package de.invesdwin.context.persistence.timeseriesdb.buffer;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.IReverseCloseableIterable;
import de.invesdwin.util.collections.iterable.skip.ASkippingIterator;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.streams.buffer.MemoryMappedFile;
import de.invesdwin.util.time.date.FDate;

@ThreadSafe
public class IterableFileBufferCacheResult<V> implements IFileBufferCacheResult<V> {

    private final IReverseCloseableIterable<V> delegate;
    private MemoryMappedFile file;

    public IterableFileBufferCacheResult(final IReverseCloseableIterable<V> delegate, final MemoryMappedFile file) {
        this.delegate = delegate;
        this.file = file;
    }

    @Override
    public ICloseableIterator<V> iterator() {
        return delegate.iterator();
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
                        throw new FastNoSuchElementException("getRangeValues reached end");
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
                        throw new FastNoSuchElementException("getRangeValues reached end");
                    }
                    return false;
                }
            };
        }
    }

    @Override
    public ICloseableIterator<V> reverseIterator() {
        return delegate.reverseIterator();
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
                        throw new FastNoSuchElementException("getRangeValues reached end");
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
                        throw new FastNoSuchElementException("getRangeValues reached end");
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
        Lists.toListWithoutHasNext(delegate, toList);
    }

    @Override
    public void close() {
        if (file != null) {
            file.decrementRefCount();
            file = null;
        }
    }

}
