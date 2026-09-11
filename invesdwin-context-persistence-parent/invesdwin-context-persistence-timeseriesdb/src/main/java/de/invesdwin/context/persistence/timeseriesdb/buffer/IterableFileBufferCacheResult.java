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
    public ICloseableIterator<V> iterator(final Function<V, FDate> extractEndTime, final FDate low, final FDate high) {
        if (low == null && high == null) {
            return iterator();
        } else if (low == null) {
            return new ASkippingIterator<V>(iterator()) {
                @Override
                protected boolean skip(final V element) {
                    final FDate time = extractEndTime.apply(element);
                    if (time.isAfterNotNullSafe(high)) {
                        throw FastNoSuchElementException.getInstance("getRangeValues reached end");
                    }
                    return false;
                }
            };
        } else if (high == null) {
            return new ASkippingIterator<V>(iterator()) {
                @Override
                protected boolean skip(final V element) {
                    final FDate time = extractEndTime.apply(element);
                    if (time.isBeforeNotNullSafe(low)) {
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
                    if (time.isBeforeNotNullSafe(low)) {
                        return true;
                    } else if (time.isAfterNotNullSafe(high)) {
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
    public ICloseableIterator<V> reverseIterator(final Function<V, FDate> extractEndTime, final FDate high,
            final FDate low) {
        if (high == null && low == null) {
            return reverseIterator();
        } else if (high == null) {
            return new ASkippingIterator<V>(reverseIterator()) {
                @Override
                protected boolean skip(final V element) {
                    final FDate time = extractEndTime.apply(element);
                    if (time.isBeforeNotNullSafe(low)) {
                        throw FastNoSuchElementException.getInstance("getRangeValues reached end");
                    }
                    return false;
                }
            };
        } else if (low == null) {
            return new ASkippingIterator<V>(reverseIterator()) {
                @Override
                protected boolean skip(final V element) {
                    final FDate time = extractEndTime.apply(element);
                    if (time.isAfterNotNullSafe(high)) {
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
                    if (time.isAfterNotNullSafe(high)) {
                        return true;
                    } else if (time.isBeforeNotNullSafe(low)) {
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
        int curIndex = -1;
        try (ICloseableIterator<V> it = iterator()) {
            while (true) {
                final V newValue = it.next();
                curIndex++;
                final FDate newValueTime = extractEndTime.apply(newValue);
                if (newValueTime.isAfterNotNullSafe(key)) {
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
        return latestValue;
    }

    @Override
    public V getLatestValueOrFallback(final Function<V, FDate> extractEndTime, final FDate key) {
        V latestValue = null;
        V firstValue = null;
        int curIndex = -1;
        try (ICloseableIterator<V> it = iterator()) {
            while (true) {
                final V newValue = it.next();
                curIndex++;
                if (firstValue == null) {
                    firstValue = newValue;
                }
                final FDate newValueTime = extractEndTime.apply(newValue);
                if (newValueTime.isAfterNotNullSafe(key)) {
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
            return latestValue;
        } else if (firstValue != null) {
            latestValueByIndex = firstValue;
            latestValueIndexByIndex = 0;
            return firstValue;
        }

        return null;
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
                if (newValueTime.isAfterNotNullSafe(key)) {
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
    public int getLatestValueIndexOrFallback(final Function<V, FDate> extractEndTime, final FDate key) {
        int curIndex = -1;
        boolean hasFirst = false;
        V latestValue = null;
        V firstValue = null;
        try (ICloseableIterator<V> it = iterator()) {
            while (true) {
                final V newValue = it.next();
                if (!hasFirst) {
                    firstValue = newValue;
                    hasFirst = true;
                }
                curIndex++;
                final FDate newValueTime = extractEndTime.apply(newValue);
                if (newValueTime.isAfterNotNullSafe(key)) {
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
            return curIndex;
        } else if (hasFirst) {
            latestValueByIndex = firstValue;
            latestValueIndexByIndex = 0;
            return 0;
        }
        return -1;
    }

    @Override
    public int size(final Function<V, FDate> extractEndTime, final FDate from, final FDate to) {
        int size = 0;
        try (ICloseableIterator<V> it = iterator()) {
            while (true) {
                final V newValue = it.next();
                final FDate newValueTime = extractEndTime.apply(newValue);
                if (newValueTime.isBeforeNotNullSafe(from)) {
                    continue;
                } else if (newValueTime.isAfterNotNullSafe(to)) {
                    break;
                } else {
                    size++;
                }
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
        return size;
    }

    @Override
    public V getLatestValue(final int index) {
        if (index < 0) {
            return null;
        }
        if (latestValueIndexByIndex == index) {
            return latestValueByIndex;
        }
        V latestValue = null;
        try (ICloseableIterator<V> it = iterator()) {
            int curIndex = -1;
            while (true) {
                final V newValue = it.next();
                curIndex++;
                if (curIndex == index) {
                    latestValue = newValue;
                    break;
                }
            }
        } catch (final NoSuchElementException e) {
            // End reached before index -> Out of bounds
            return null;
        }
        if (latestValue != null) {
            latestValueByIndex = latestValue;
            latestValueIndexByIndex = index;
        }
        return latestValue;
    }

    @Override
    public V getLatestValueOrFallback(final int index) {
        if (index < 0) {
            return getLatestValue(0);
        }
        if (latestValueIndexByIndex == index) {
            return latestValueByIndex;
        }
        int curIndex = -1;
        V latestValue = null;
        V lastValueSeen = null;
        try (ICloseableIterator<V> it = iterator()) {
            while (true) {
                final V newValue = it.next();
                lastValueSeen = newValue;
                curIndex++;
                if (curIndex == index) {
                    latestValue = newValue;
                    break;
                }
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }

        if (latestValue != null) {
            latestValueByIndex = latestValue;
            latestValueIndexByIndex = index;
            return latestValue;
        }
        // If we exhausted the iterator before reaching the index, return the last value seen
        if (lastValueSeen != null) {
            latestValueByIndex = lastValueSeen;
            latestValueIndexByIndex = curIndex;
            return lastValueSeen;
        }
        return null;
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