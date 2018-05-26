package de.invesdwin.context.persistence.leveldb.timeseries.storage;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang3.builder.CompareToBuilder;

import de.invesdwin.util.time.fdate.FDate;

@Immutable
public class ShiftUnitsRangeKey implements Comparable<Object> {

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
    public int compareTo(final Object o) {
        if (o instanceof ShiftUnitsRangeKey) {
            final ShiftUnitsRangeKey cO = (ShiftUnitsRangeKey) o;
            return new CompareToBuilder().append(rangeKey, cO.rangeKey)
                    .append(shiftUnits, cO.shiftUnits)
                    .toComparison();
        } else {
            return 1;
        }
    }

}
