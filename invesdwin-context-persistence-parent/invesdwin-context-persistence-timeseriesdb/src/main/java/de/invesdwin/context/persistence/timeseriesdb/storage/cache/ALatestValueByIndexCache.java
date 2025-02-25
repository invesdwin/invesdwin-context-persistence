package de.invesdwin.context.persistence.timeseriesdb.storage.cache;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.collections.circular.CircularGenericArrayQueue;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.time.date.BisectDuplicateKeyHandling;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;

@ThreadSafe
public abstract class ALatestValueByIndexCache<V> {

    private static final int PREV_PREV_INDEX = 0;
    private static final int PREV_INDEX = 1;
    private static final int CUR_INDEX = 2;
    private static final int NEXT_INDEX = 3;
    private static final int NEXT_NEXT_INDEX = 4;

    private final CircularGenericArrayQueue<V> values = new CircularGenericArrayQueue<>(5);
    private long curStorageIndex;
    private volatile int prevResetIndex = getLastResetIndex() - 1;

    protected abstract int getLastResetIndex();

    protected abstract long getLatestValueIndex(FDate key);

    protected abstract V getLatestValue(long index);

    protected abstract FDate extractEndTime(V value);

    private FDate getKey(final int index) {
        final V value = values.get(index);
        return extractEndTime(value);
    }

    public V getLatestValueByIndex(final FDate date) {
        final int lastResetIndex = getLastResetIndex();
        if (prevResetIndex != lastResetIndex) {
            return init(date, lastResetIndex);
        } else if (!date.isBetweenInclusiveNotNullSafe(getKey(PREV_PREV_INDEX), getKey(NEXT_NEXT_INDEX))) {
            return init(date, lastResetIndex);
        }
        final int bisect = FDates.bisect(this::extractEndTime, values, date, BisectDuplicateKeyHandling.UNDEFINED);
        switch (bisect) {
        case PREV_PREV_INDEX: {
            final V prevPrevValue = values.get(PREV_PREV_INDEX);
            //now for sure move backward
            moveBackward();
            return prevPrevValue;
        }
        case PREV_INDEX: {
            return values.get(PREV_INDEX);
        }
        case CUR_INDEX: {
            return values.get(CUR_INDEX);
        }
        case NEXT_INDEX: {
            final V nextValue = values.get(NEXT_INDEX);
            final FDate nextKey = extractEndTime(nextValue);
            if (date.isAfterNotNullSafe(nextKey)) {
                //move forward a bit earlier to increase hit rate
                moveForward();
            }
            return nextValue;
        }
        case NEXT_NEXT_INDEX: {
            final V nextNextValue = values.get(NEXT_NEXT_INDEX);
            //now for sure move forward
            moveForward();
            return nextNextValue;
        }
        default:
            throw UnknownArgumentException.newInstance(Integer.class, bisect);
        }
    }

    private void moveForward() {
        if (getKey(NEXT_NEXT_INDEX).equalsNotNullSafe(getKey(NEXT_INDEX))) {
            //no more data to move to
            return;
        }
        final long nextNextNextStorageIndex = curStorageIndex + 3L;
        curStorageIndex++;
        final V nextNextValue = getLatestValue(nextNextNextStorageIndex);
        values.circularAdd(nextNextValue);
    }

    private void moveBackward() {
        if (getKey(PREV_PREV_INDEX).equalsNotNullSafe(getKey(PREV_INDEX))) {
            //no more data to move to
            return;
        }
        final long prevPrevPrevStorageIndex = curStorageIndex - 3L;
        curStorageIndex--;
        final V prevPrevValue = getLatestValue(prevPrevPrevStorageIndex);
        values.circularPrepend(prevPrevValue);
    }

    private V init(final FDate date, final int lastResetIndex) {
        curStorageIndex = getLatestValueIndex(date);
        final V curValue = getLatestValue(curStorageIndex);
        if (curValue != null) {
            final long prevPrevStorageIndex = curStorageIndex - 2L;
            final V prevPrevValue = getLatestValue(prevPrevStorageIndex);
            final long prevStorageIndex = curStorageIndex - 1L;
            final V prevValue = getLatestValue(prevStorageIndex);
            final long nextStorageIndex = curStorageIndex + 1L;
            final V nextValue = getLatestValue(nextStorageIndex);
            final long nextNextStorageIndex = curStorageIndex + 2L;
            final V nextNextValue = getLatestValue(nextNextStorageIndex);

            values.pretendClear();
            values.add(prevPrevValue);
            values.add(prevValue);
            values.add(curValue);
            values.add(nextValue);
            values.add(nextNextValue);

            prevResetIndex = lastResetIndex;
            return curValue;
        } else {
            prevResetIndex = lastResetIndex - 1;
            return null;
        }
    }

}
