package de.invesdwin.context.persistence.timeseriesdb.storage.cache;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.time.date.FDate;

@ThreadSafe
public abstract class ALatestValueByIndexCache<V> {

    private long prevIndex;
    private FDate prevKey;
    private V prevValue;
    private long curIndex;
    private FDate curKey;
    private V curValue;
    private long nextIndex;
    private FDate nextKey;
    private V nextValue;
    private volatile int prevResetIndex = getLastResetIndex() - 1;

    protected abstract int getLastResetIndex();

    protected abstract long getLatestValueIndex(FDate key);

    protected abstract V getLatestValue(long index);

    protected abstract FDate extractEndTime(V value);

    public V getLatestValueByIndex(final FDate date) {
        final int lastResetIndex = getLastResetIndex();
        if (prevResetIndex != lastResetIndex || !date.isBetweenInclusive(prevKey, nextKey)) {
            init(date, lastResetIndex);
            return curValue;
        } else if (date.isBetweenInclusiveNotNullSafe(curKey, nextKey)) {
            if (date.equalsNotNullSafe(nextKey)) {
                moveForward();
            }
            return curValue;
        } else if (date.isBetweenInclusiveNotNullSafe(prevKey, curKey)) {
            if (date.equalsNotNullSafe(prevKey)) {
                moveBackward();
                return curValue;
            } else {
                return prevValue;
            }
        } else {
            throw new IllegalStateException("Unexpected request: " + date);
        }
    }

    private void moveForward() {
        prevIndex = curIndex;
        prevValue = curValue;
        prevKey = curKey;
        curIndex = nextIndex;
        curValue = nextValue;
        curKey = nextKey;
        nextIndex = curIndex + 1;
        nextValue = getLatestValue(nextIndex);
        nextKey = extractEndTime(nextValue);
    }

    private void moveBackward() {
        nextIndex = curIndex;
        nextValue = curValue;
        nextKey = curKey;
        curIndex = prevIndex;
        curValue = prevValue;
        curKey = prevKey;
        prevIndex = curIndex - 1;
        prevValue = getLatestValue(prevIndex);
        prevKey = extractEndTime(prevValue);
    }

    private void init(final FDate date, final int lastResetIndex) {
        curIndex = getLatestValueIndex(date);
        curValue = getLatestValue(curIndex);
        if (curValue != null) {
            curKey = extractEndTime(curValue);
            prevIndex = curIndex - 1;
            prevValue = getLatestValue(prevIndex);
            prevKey = extractEndTime(prevValue);
            nextIndex = curIndex + 1;
            nextValue = getLatestValue(nextIndex);
            nextKey = extractEndTime(nextValue);
            prevResetIndex = lastResetIndex;
        } else {
            prevResetIndex = lastResetIndex - 1;
        }
    }

}
