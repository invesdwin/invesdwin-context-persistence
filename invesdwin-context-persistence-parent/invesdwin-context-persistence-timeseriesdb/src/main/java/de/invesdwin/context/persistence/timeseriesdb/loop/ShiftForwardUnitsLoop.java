package de.invesdwin.context.persistence.timeseriesdb.loop;

import java.util.NoSuchElementException;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class ShiftForwardUnitsLoop<V> {

    private final FDate date;
    private final int shiftForwardUnits;
    private final Function<V, FDate> extractEndTimeF;
    private V nextValue;
    private int shiftForwardRemaining;

    public ShiftForwardUnitsLoop(final FDate date, final int shiftForwardUnits,
            final Function<V, FDate> extractEndTimeF) {
        this.date = date;
        this.shiftForwardUnits = shiftForwardUnits;
        this.shiftForwardRemaining = shiftForwardUnits;
        this.extractEndTimeF = extractEndTimeF;
    }

    public int getShiftForwardRemaining() {
        return shiftForwardRemaining;
    }

    public void skip(final int count) {
        this.shiftForwardRemaining -= count;
    }

    public V getNextValue() {
        return nextValue;
    }

    public void loop(final ICloseableIterable<? extends V> iterable) {
        loop(iterable.iterator());
    }

    public void loop(final ICloseableIterator<? extends V> iterator) {
        try (ICloseableIterator<? extends V> rangeValues = iterator) {
            /*
             * workaround for determining next key with multiple values at the same millisecond (without this workaround
             * we would return a duplicate that might produce an endless loop)
             */
            if (shiftForwardUnits == 0) {
                while (shiftForwardRemaining == 0) {
                    final V nextNextValue = rangeValues.next();
                    final FDate nextNextValueKey = extractEndTimeF.apply(nextNextValue);
                    if (!nextNextValueKey.isBeforeNotNullSafe(date)) {
                        nextValue = nextNextValue;
                        shiftForwardRemaining--;
                    }
                }
            } else if (shiftForwardUnits == 1) {
                while (shiftForwardRemaining >= 0) {
                    final V nextNextValue = rangeValues.next();
                    final FDate nextNextValueKey = extractEndTimeF.apply(nextNextValue);
                    if (shiftForwardRemaining == 1 || date.isBeforeNotNullSafe(nextNextValueKey)) {
                        nextValue = nextNextValue;
                        shiftForwardRemaining--;
                    }
                }
            } else {
                while (shiftForwardRemaining >= 0) {
                    final V nextNextValue = rangeValues.next();
                    nextValue = nextNextValue;
                    shiftForwardRemaining--;
                }
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
    }

}
