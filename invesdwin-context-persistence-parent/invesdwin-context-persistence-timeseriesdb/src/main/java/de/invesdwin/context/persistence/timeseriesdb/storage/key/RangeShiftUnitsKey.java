package de.invesdwin.context.persistence.timeseriesdb.storage.key;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.time.date.FDate;

@Immutable
public class RangeShiftUnitsKey implements Comparable<Object> {

    private final int shiftUnits;
    private final FDate rangeKey;

    public RangeShiftUnitsKey(final FDate rangeKey, final int shiftUnits) {
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
        if (obj instanceof RangeShiftUnitsKey) {
            final RangeShiftUnitsKey cObj = (RangeShiftUnitsKey) obj;
            return Objects.equals(rangeKey, cObj.rangeKey) && Objects.equals(shiftUnits, cObj.shiftUnits);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(RangeShiftUnitsKey.class, rangeKey, shiftUnits);
    }

    @Override
    public int compareTo(final Object o) {
        if (o instanceof RangeShiftUnitsKey) {
            final RangeShiftUnitsKey cO = (RangeShiftUnitsKey) o;
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

    @Override
    public String toString() {
        return Objects.toString(this);
    }

}
