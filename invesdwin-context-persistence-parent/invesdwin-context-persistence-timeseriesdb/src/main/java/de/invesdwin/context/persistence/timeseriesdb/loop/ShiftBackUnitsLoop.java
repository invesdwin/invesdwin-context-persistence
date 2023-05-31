package de.invesdwin.context.persistence.timeseriesdb.loop;

import java.util.NoSuchElementException;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class ShiftBackUnitsLoop<V> {

    private final FDate date;
    private final int shiftBackUnits;
    private final Function<V, FDate> extractEndTimeF;
    private V prevValue;
    private int shiftBackRemaining;

    public ShiftBackUnitsLoop(final FDate date, final int shiftBackUnits, final Function<V, FDate> extractEndTimeF) {
        this.date = date;
        this.shiftBackUnits = shiftBackUnits;
        this.shiftBackRemaining = shiftBackUnits;
        this.extractEndTimeF = extractEndTimeF;
    }

    public int getShiftBackRemaining() {
        return shiftBackRemaining;
    }

    public void skip(final int count) {
        this.shiftBackRemaining -= count;
    }

    public V getPrevValue() {
        return prevValue;
    }

    public void loop(final ICloseableIterable<? extends V> iterable) {
        loop(iterable.iterator());
    }

    public void loop(final ICloseableIterator<? extends V> iterator) {
        try (ICloseableIterator<? extends V> rangeValuesReverse = iterator) {
            /*
             * workaround for determining next key with multiple values at the same millisecond (without this workaround
             * we would return a duplicate that might produce an endless loop)
             */
            if (shiftBackUnits == 0) {
                while (shiftBackRemaining == 0) {
                    final V prevPrevValue = rangeValuesReverse.next();
                    final FDate prevPrevValueKey = extractEndTimeF.apply(prevPrevValue);
                    if (!prevPrevValueKey.isAfterNotNullSafe(date)) {
                        prevValue = prevPrevValue;
                        shiftBackRemaining--;
                    }
                }
            } else if (shiftBackUnits == 1) {
                while (shiftBackRemaining >= 0) {
                    final V prevPrevValue = rangeValuesReverse.next();
                    final FDate prevPrevValueKey = extractEndTimeF.apply(prevPrevValue);
                    if (shiftBackRemaining == 1 || date.isAfterNotNullSafe(prevPrevValueKey)) {
                        prevValue = prevPrevValue;
                        shiftBackRemaining--;
                    }
                }
            } else {
                while (shiftBackRemaining >= 0) {
                    final V prevPrevValue = rangeValuesReverse.next();
                    prevValue = prevPrevValue;
                    shiftBackRemaining--;
                }
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
    }

}
