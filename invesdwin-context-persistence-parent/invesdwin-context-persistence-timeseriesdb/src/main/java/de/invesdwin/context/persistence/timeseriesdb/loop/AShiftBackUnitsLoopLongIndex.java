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
        if (prevValue == null) {
            return -1L;
        } else {
            return prevValueIndex;
        }
    }

    public V getPrevValue() {
        return prevValue;
    }

    protected abstract V getLatestValue(long index);

    protected abstract long getLatestValueIndex(FDate date);

    protected abstract FDate extractEndTime(V value);

    protected abstract long size();

    public void loop() {
        while (loopTry()) {
            shiftBackRemaining = shiftBackUnits;
            prevValue = null;
            prevValueIndex = -1L;
        }
    }

    private boolean loopTry() {
        final long size = size();
        long prevPrevValueIndex = getLatestValueIndex(date);

        if (shiftBackUnits == 0) {
            while (shiftBackRemaining == 0 && prevPrevValueIndex >= 0) {
                final V prevPrevValue = getLatestValue(prevPrevValueIndex);
                final FDate prevPrevValueKey = extractEndTime(prevPrevValue);
                if (!prevPrevValueKey.isAfterNotNullSafe(date)) {
                    prevValue = prevPrevValue;
                    prevValueIndex = prevPrevValueIndex;
                    shiftBackRemaining--;
                }
                prevPrevValueIndex--;
            }
        } else if (shiftBackUnits == 1) {
            while (shiftBackRemaining >= 0 && prevPrevValueIndex >= 0) {
                final V prevPrevValue = getLatestValue(prevPrevValueIndex);
                final FDate prevPrevValueKey = extractEndTime(prevPrevValue);
                if (!prevPrevValueKey.isAfterNotNullSafe(date)) {
                    if (shiftBackRemaining == 1 || date.isAfterNotNullSafe(prevPrevValueKey)) {
                        prevValue = prevPrevValue;
                        prevValueIndex = prevPrevValueIndex;
                        shiftBackRemaining--;
                    }
                }
                prevPrevValueIndex--;
            }
        } else {
            while (shiftBackRemaining >= 0 && prevPrevValueIndex >= 0) {
                final V prevPrevValue = getLatestValue(prevPrevValueIndex);
                final FDate prevPrevValueKey = extractEndTime(prevPrevValue);
                if (!prevPrevValueKey.isAfterNotNullSafe(date)) {
                    //just skip ahead without another loop iteration since we already found our starting point
                    prevPrevValueIndex -= shiftBackRemaining;
                    if (prevPrevValueIndex < 0L) {
                        prevPrevValueIndex = 0L;
                    }
                    shiftBackRemaining = -1;
                    prevValue = getLatestValue(prevPrevValueIndex);
                    prevValueIndex = prevPrevValueIndex;
                    break;
                }
                prevPrevValueIndex--;
            }
        }
        return size != size();
    }

}
