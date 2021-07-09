package de.invesdwin.context.persistence.timeseries.ezdb;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;

@ThreadSafe
public abstract class AHourlyExpiringDelegateRangeTable<H, R, V> extends ADelegateRangeTable<H, R, V> {

    public AHourlyExpiringDelegateRangeTable(final String name) {
        super(name);
    }

    @Override
    protected boolean shouldPurgeTable() {
        final FDate tableCreationTime = getTableCreationTime();
        return tableCreationTime != null && !FDates.isSameJulianHour(tableCreationTime, new FDate());
    }

}
