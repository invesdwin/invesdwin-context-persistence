package de.invesdwin.context.persistence.timeseriesdb.loop;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public abstract class AShiftForwardUnitsLoopIntIndex<V> {

    private final FDate date;
    private final int shiftForwardUnits;
    private int nextValueIndex = -1;
    private V nextValue;
    private int shiftForwardRemaining;

    public AShiftForwardUnitsLoopIntIndex(final FDate date, final int shiftForwardUnits) {
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

    public int getNextValueIndex() {
        return nextValueIndex;
    }

    public V getNextValue() {
        return nextValue;
    }

    protected abstract V getValue(int index);

    protected abstract int getLatestValueIndex(FDate date);

    protected abstract FDate extractEndTime(V value);

    protected abstract int size();

    public void loop() {
        final int size = size();
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
