package de.invesdwin.context.persistence.timeseriesdb.storage.key;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.time.date.FDate;

@Immutable
public class HashRangeShiftUnitsKey implements Comparable<Object> {

    private final String hashKey;
    private final FDate rangeKey;
    private final int shiftUnits;

    public HashRangeShiftUnitsKey(final String hashKey, final FDate rangeKey, final int shiftUnits) {
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
        if (obj instanceof HashRangeShiftUnitsKey) {
            final HashRangeShiftUnitsKey cObj = (HashRangeShiftUnitsKey) obj;
            return Objects.equals(hashKey, cObj.hashKey) && Objects.equals(rangeKey, cObj.rangeKey)
                    && Objects.equals(shiftUnits, cObj.shiftUnits);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(HashRangeShiftUnitsKey.class, hashKey, rangeKey, shiftUnits);
    }

    @Override
    public int compareTo(final Object o) {
        if (o instanceof HashRangeShiftUnitsKey) {
            final HashRangeShiftUnitsKey cO = (HashRangeShiftUnitsKey) o;
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

    @Override
    public String toString() {
        return Objects.toString(this);
    }

}
