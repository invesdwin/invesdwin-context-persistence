package de.invesdwin.context.persistence.ezdb.table.expiring;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.ezdb.table.ADelegateTable;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;

@ThreadSafe
public abstract class ADailyExpiringDelegateTable<H, V> extends ADelegateTable<H, V> {

    public ADailyExpiringDelegateTable(final String name) {
        super(name);
    }

    @Override
    protected boolean shouldPurgeTable() {
        final FDate tableCreationTime = getTableCreationTime();
        return tableCreationTime != null && !FDates.isSameJulianDay(tableCreationTime, new FDate());
    }

}
