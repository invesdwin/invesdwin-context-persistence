package de.invesdwin.context.persistence.leveldb;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.time.fdate.FDate;

@ThreadSafe
public abstract class ADailyExpiringDelegateRangeTable<H, R, V> extends ADelegateRangeTable<H, R, V> {

    public ADailyExpiringDelegateRangeTable(final String name) {
        super(name);
    }

    @Override
    protected boolean shouldPurgeTable() {
        final FDate tableCreationTime = getTableCreationTime();
        return tableCreationTime != null && !FDate.isSameJulianDay(tableCreationTime, new FDate());
    }

}
