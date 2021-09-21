package de.invesdwin.context.persistence.timeseriesdb.storage;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.bean.AValueObject;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.time.date.FDate;

@Immutable
public class ShiftUnitsHashKey extends AValueObject implements Comparable<Object> {

    private final String hashKey;
    private final FDate rangeKey;
    private final int shiftUnits;

    public ShiftUnitsHashKey(final String hashKey, final FDate rangeKey, final int shiftUnits) {
        this.hashKey = hashKey;
        this.rangeKey = rangeKey;
        this.shiftUnits = shiftUnits;
    }

    public String getHashKey() {
        return hashKey;
    }

    public FDate getRangeKey() {
        return rangeKey;
    }

    public int getShiftUnits() {
        return shiftUnits;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof ShiftUnitsHashKey) {
            final ShiftUnitsHashKey cObj = (ShiftUnitsHashKey) obj;
            return Objects.equals(hashKey, cObj.hashKey) && Objects.equals(rangeKey, cObj.rangeKey)
                    && Objects.equals(shiftUnits, cObj.shiftUnits);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(ShiftUnitsHashKey.class, hashKey, shiftUnits, rangeKey);
    }

    @Override
    public int compareTo(final Object o) {
        if (o instanceof ShiftUnitsHashKey) {
            final ShiftUnitsHashKey cO = (ShiftUnitsHashKey) o;
            int compare = hashKey.compareTo(cO.hashKey);
            if (compare != 0) {
                return compare;
            }
            compare = rangeKey.compareToNotNullSafe(cO.rangeKey);
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
