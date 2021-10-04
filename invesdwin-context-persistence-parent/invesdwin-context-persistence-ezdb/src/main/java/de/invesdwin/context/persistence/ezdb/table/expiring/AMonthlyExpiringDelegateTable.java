package de.invesdwin.context.persistence.ezdb.table.expiring;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.ezdb.table.range.ADelegateRangeTable;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;

@ThreadSafe
public abstract class AMonthlyExpiringDelegateTable<H, R, V> extends ADelegateRangeTable<H, R, V> {

    public AMonthlyExpiringDelegateTable(final String name) {
        super(name);
    }

    @Override
    protected boolean shouldPurgeTable() {
        final FDate tableCreationTime = getTableCreationTime();
        return tableCreationTime != null && !FDates.isSameMonth(tableCreationTime, new FDate());
    }

}
