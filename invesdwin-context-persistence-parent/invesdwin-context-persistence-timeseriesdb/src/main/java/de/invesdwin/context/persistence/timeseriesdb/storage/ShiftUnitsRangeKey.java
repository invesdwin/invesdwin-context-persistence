package de.invesdwin.context.persistence.timeseriesdb.storage;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.bean.AValueObject;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.time.date.FDate;

@Immutable
public class ShiftUnitsRangeKey extends AValueObject implements Comparable<Object> {

    private final int shiftUnits;
    private final FDate rangeKey;

    public ShiftUnitsRangeKey(final FDate rangeKey, final int shiftUnits) {
        this.rangeKey = rangeKey;
        this.shiftUnits = shiftUnits;
    }

    public int getShiftUnits() {
        return shiftUnits;
    }

    public FDate getRangeKey() {
        return rangeKey;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof ShiftUnitsRangeKey) {
            final ShiftUnitsRangeKey cObj = (ShiftUnitsRangeKey) obj;
            return Objects.equals(rangeKey, cObj.rangeKey) && Objects.equals(shiftUnits, cObj.shiftUnits);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(ShiftUnitsRangeKey.class, shiftUnits, rangeKey);
    }

    @Override
    public int compareTo(final Object o) {
        if (o instanceof ShiftUnitsRangeKey) {
            final ShiftUnitsRangeKey cO = (ShiftUnitsRangeKey) o;
            int compare = rangeKey.compareToNotNullSafe(cO.rangeKey);
            if (compare != 0) {
                return compare;
            }
            compare = Integer.compare(shiftUnits, cO.shiftUnits);
            return compare;
        } else {
            return 1;
        }
    }

}
