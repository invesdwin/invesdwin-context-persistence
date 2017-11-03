package de.invesdwin.context.persistence.leveldb.timeseries.storage;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.time.fdate.FDate;

@Immutable
public class ShiftUnitsRangeKey {

    private final int shiftUnits;
    private final FDate rangeKey;

    public ShiftUnitsRangeKey(final int shiftUnits, final FDate rangeKey) {
        this.shiftUnits = shiftUnits;
        this.rangeKey = rangeKey;
    }

    public int getShiftUnits() {
        return shiftUnits;
    }

    public FDate getRangeKey() {
        return rangeKey;
    }

}
