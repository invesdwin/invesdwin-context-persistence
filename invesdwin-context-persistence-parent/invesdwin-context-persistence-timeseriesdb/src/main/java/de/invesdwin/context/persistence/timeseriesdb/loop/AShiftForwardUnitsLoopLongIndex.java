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
        return nextValueIndex;
    }

    public V getNextValue() {
        return nextValue;
    }

    protected abstract V getValue(long index);

    protected abstract long getLatestValueIndex(FDate date);

    protected abstract FDate extractEndTime(V value);

    protected abstract long size();

    public void loop() {
        final long size = size();
        nextValueIndex = getLatestValueIndex(date);
        if (shiftForwardUnits == 0) {
            while (shiftForwardRemaining == 0 && nextValueIndex < size) {
                final V nextNextValue = getValue(nextValueIndex);
                final FDate nextNextValueKey = extractEndTime(nextNextValue);
                if (!nextNextValueKey.isBeforeNotNullSafe(date)) {
                    nextValue = nextNextValue;
                    shiftForwardRemaining--;
                }
                nextValueIndex++;
            }
        } else if (shiftForwardUnits == 1) {
            while (shiftForwardRemaining >= 0 && nextValueIndex < size) {
                final V nextNextValue = getValue(nextValueIndex);
                final FDate nextNextValueKey = extractEndTime(nextNextValue);
                if (!nextNextValueKey.isBeforeNotNullSafe(date)) {
                    if (shiftForwardRemaining == 1 || date.isBeforeNotNullSafe(nextNextValueKey)) {
                        nextValue = nextNextValue;
                        shiftForwardRemaining--;
                    }
                }
                nextValueIndex++;
            }
        } else {
            while (shiftForwardRemaining >= 0 && nextValueIndex < size) {
                final V nextNextValue = getValue(nextValueIndex);
                final FDate nextNextValueKey = extractEndTime(nextNextValue);
                if (!nextNextValueKey.isBeforeNotNullSafe(date)) {
                    nextValue = nextNextValue;
                    //just skip ahead without another loop iteration since we already found our starting point
                    nextValueIndex += shiftForwardRemaining;
                    shiftForwardRemaining = -1;
                    nextValue = getValue(nextValueIndex);
                    break;
                } else {
                    nextValueIndex++;
                }
            }
        }
    }

}
