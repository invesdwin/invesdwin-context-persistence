package de.invesdwin.context.persistence.timeseriesdb.loop;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public abstract class AShiftBackUnitsLoopLongIndex<V> {

    private final FDate date;
    private final int shiftBackUnits;
    private long prevValueIndex = -1L;
    private V prevValue;
    private int shiftBackRemaining;

    public AShiftBackUnitsLoopLongIndex(final FDate date, final int shiftBackUnits) {
        this.date = date;
        this.shiftBackUnits = shiftBackUnits;
        this.shiftBackRemaining = shiftBackUnits;
    }

    public int getShiftBackRemaining() {
        return shiftBackRemaining;
    }

    public void skip(final int count) {
        this.shiftBackRemaining -= count;
    }

    public long getPrevValueIndex() {
        return prevValueIndex;
    }

    public V getPrevValue() {
        return prevValue;
    }

    protected abstract V getValue(long index);

    protected abstract long getLatestValueIndex(FDate date);

    protected abstract FDate extractEndTime(V value);

    public void loop() {
        prevValueIndex = getLatestValueIndex(date);

        if (shiftBackUnits == 0) {
            while (shiftBackRemaining == 0 && prevValueIndex >= 0) {
                final V prevPrevValue = getValue(prevValueIndex);
                final FDate prevPrevValueKey = extractEndTime(prevPrevValue);
                if (!prevPrevValueKey.isAfterNotNullSafe(date)) {
                    prevValue = prevPrevValue;
                    shiftBackRemaining--;
                }
                prevValueIndex--;
            }
        } else if (shiftBackUnits == 1) {
            while (shiftBackRemaining >= 0 && prevValueIndex >= 0) {
                final V prevPrevValue = getValue(prevValueIndex);
                final FDate prevPrevValueKey = extractEndTime(prevPrevValue);
                if (!prevPrevValueKey.isAfterNotNullSafe(date)) {
                    if (shiftBackRemaining == 1 || date.isAfterNotNullSafe(prevPrevValueKey)) {
                        prevValue = prevPrevValue;
                        shiftBackRemaining--;
                    }
                }
                prevValueIndex--;
            }
        } else {
            while (shiftBackRemaining >= 0 && prevValueIndex >= 0) {
                final V prevPrevValue = getValue(prevValueIndex);
                final FDate prevPrevValueKey = extractEndTime(prevPrevValue);
                if (!prevPrevValueKey.isAfterNotNullSafe(date)) {
                    prevValue = prevPrevValue;
                    //just skip ahead without another loop iteration since we already found our starting point
                    prevValueIndex -= shiftBackRemaining;
                    if (prevValueIndex < 0) {
                        prevValueIndex = 0;
                    }
                    shiftBackRemaining = -1;
                    prevValue = getValue(prevValueIndex);
                    break;
                }
                prevValueIndex--;
            }
        }
    }

}
