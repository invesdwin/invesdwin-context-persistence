package de.invesdwin.context.persistence.leveldb.timeseries.storage;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.lang.ADelegateComparator;
import de.invesdwin.util.time.fdate.FDate;

@Immutable
public class ShiftUnitsRangeKey implements Comparable<Object> {

    public static final ADelegateComparator<ShiftUnitsRangeKey> COMPARATOR = new ADelegateComparator<ShiftUnitsRangeKey>() {
        @Override
        protected Comparable<?> getCompareCriteria(final ShiftUnitsRangeKey e) {
            return e.getRangeKey();
        }
    };

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

    @Override
    public int compareTo(final Object o) {
        return COMPARATOR.compare(this, o);
    }

}
