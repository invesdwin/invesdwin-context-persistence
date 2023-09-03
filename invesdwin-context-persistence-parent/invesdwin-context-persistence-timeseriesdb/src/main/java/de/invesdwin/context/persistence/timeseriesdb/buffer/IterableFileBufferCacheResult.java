package de.invesdwin.context.persistence.timeseriesdb.buffer;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.IReverseCloseableIterable;
import de.invesdwin.util.collections.iterable.skip.ASkippingIterator;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class IterableFileBufferCacheResult<V> implements IFileBufferCacheResult<V> {

    private final IReverseCloseableIterable<V> delegate;
    private V latestValueByIndex;
    private int latestValueIndexByIndex = -1;

    public IterableFileBufferCacheResult(final IReverseCloseableIterable<V> delegate) {
        this.delegate = delegate;
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
    public int getLatestValueIndex(final Function<V, FDate> extractEndTime, final FDate key) {
        int curIndex = -1;
        V latestValue = null;
        try (ICloseableIterator<V> it = iterator()) {
            while (true) {
                final V newValue = it.next();
                curIndex++;
                final FDate newValueTime = extractEndTime.apply(newValue);
                if (newValueTime.isAfter(key)) {
                    curIndex--;
                    break;
                } else {
                    latestValue = newValue;
                }
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
        if (latestValue != null) {
            latestValueByIndex = latestValue;
            latestValueIndexByIndex = curIndex;
        }
        return curIndex;
    }

    @Override
    public V getLatestValue(final int index) {
        if (index < 0) {
            return null;
        }
        if (latestValueIndexByIndex == index) {
            return latestValueByIndex;
        }
        int curIndex = -1;
        V latestValue = null;
        try (ICloseableIterator<V> it = iterator()) {
            while (true) {
                final V newValue = it.next();
                curIndex++;
                if (curIndex > index) {
                    break;
                } else {
                    latestValue = newValue;
                }
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
        if (latestValue != null) {
            latestValueByIndex = latestValue;
            latestValueIndexByIndex = index;
        }
        return latestValue;
    }

    @Override
    public void addToList(final List<V> toList) {
        Lists.toListWithoutHasNext(delegate, toList);
    }

    @Override
    public void close() {
        //noop
    }

}
