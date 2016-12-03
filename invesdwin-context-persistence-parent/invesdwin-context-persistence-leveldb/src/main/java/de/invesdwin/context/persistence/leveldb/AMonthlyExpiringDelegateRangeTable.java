package de.invesdwin.context.persistence.leveldb;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FDates;

@ThreadSafe
public abstract class AMonthlyExpiringDelegateRangeTable<H, R, V> extends ADelegateRangeTable<H, R, V> {

    public AMonthlyExpiringDelegateRangeTable(final String name) {
        super(name);
    }

    @Override
    protected boolean shouldPurgeTable() {
        final FDate tableCreationTime = getTableCreationTime();
        return tableCreationTime != null && !FDates.isSameMonth(tableCreationTime, new FDate());
    }

}
