package de.invesdwin.context.persistence.leveldb.ezdb;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FDates;

@ThreadSafe
public abstract class ADailyExpiringDelegateRangeTable<H, R, V> extends ADelegateRangeTable<H, R, V> {

    public ADailyExpiringDelegateRangeTable(final String name) {
        super(name);
    }

    @Override
    protected boolean shouldPurgeTable() {
        final FDate tableCreationTime = getTableCreationTime();
        return tableCreationTime != null && !FDates.isSameJulianDay(tableCreationTime, new FDate());
    }

}
