package de.invesdwin.context.persistence.timeseriesdb.loop;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public abstract class AShiftForwardUnitsLoopLongIndex<V> {

    private final FDate date;
    private final int shiftForwardUnits;
    private long nextValueIndex = -1L;
    private V nextValue;
    private int shiftForwardRemaining;

    public AShiftForwardUnitsLoopLongIndex(final FDate date, final int shiftForwardUnits) {
        this.date = date;
        this.shiftForwardUnits = shiftForwardUnits;
        this.shiftForwardRemaining = shiftForwardUnits;
    }

    public int getShiftForwardRemaining() {
        return shiftForwardRemaining;
    }

    public void skip(final int count) {
        this.shiftForwardRemaining -= count;
    }

    public long getNextValueIndex() {
        if (nextValue == null) {
            return -1L;
        } else {
            return nextValueIndex;
        }
    }

    public V getNextValue() {
        return nextValue;
    }

    protected abstract V getLatestValue(long index);

    protected abstract long getLatestValueIndex(FDate date);

    protected abstract FDate extractEndTime(V value);

    protected abstract long size();

    public void loop() {
        while (loopTry()) {
            shiftForwardRemaining = shiftForwardUnits;
            nextValue = null;
            nextValueIndex = -1;
        }
    }

    private boolean loopTry() {
        final long size = size();
        long nextNextValueIndex = getLatestValueIndex(date);
        if (shiftForwardUnits == 0) {
            while (shiftForwardRemaining == 0 && nextNextValueIndex < size) {
                final V nextNextValue = getLatestValue(nextNextValueIndex);
                final FDate nextNextValueKey = extractEndTime(nextNextValue);
                if (!nextNextValueKey.isBeforeNotNullSafe(date)) {
                    nextValue = nextNextValue;
                    nextValueIndex = nextNextValueIndex;
                    shiftForwardRemaining--;
                }
                nextNextValueIndex++;
            }
        } else if (shiftForwardUnits == 1) {
            while (shiftForwardRemaining >= 0 && nextNextValueIndex < size) {
                final V nextNextValue = getLatestValue(nextNextValueIndex);
                final FDate nextNextValueKey = extractEndTime(nextNextValue);
                if (!nextNextValueKey.isBeforeNotNullSafe(date)) {
                    if (shiftForwardRemaining == 1 || date.isBeforeNotNullSafe(nextNextValueKey)) {
                        nextValue = nextNextValue;
                        nextValueIndex = nextNextValueIndex;
                        shiftForwardRemaining--;
                    }
                }
                nextNextValueIndex++;
            }
        } else {
            while (shiftForwardRemaining >= 0 && nextNextValueIndex < size) {
                final V nextNextValue = getLatestValue(nextNextValueIndex);
                final FDate nextNextValueKey = extractEndTime(nextNextValue);
                if (!nextNextValueKey.isBeforeNotNullSafe(date)) {
                    //just skip ahead without another loop iteration since we already found our starting point
                    nextNextValueIndex += shiftForwardRemaining;
                    shiftForwardRemaining = -1;
                    nextValue = getLatestValue(nextNextValueIndex);
                    nextValueIndex = nextNextValueIndex;
                    break;
                } else {
                    nextNextValueIndex++;
                }
            }
        }
        return size != size();
    }

}
