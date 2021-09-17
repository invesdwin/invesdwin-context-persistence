package de.invesdwin.context.persistence.ezdb;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public abstract class AExpiringDelegateRangeTable<H, R, V> extends ADelegateRangeTable<H, R, V> {

    private final Duration duration;

    public AExpiringDelegateRangeTable(final String name, final Duration duration) {
        super(name);
        this.duration = duration;
    }

    @Override
    protected boolean shouldPurgeTable() {
        final FDate tableCreationTime = getTableCreationTime();
        return tableCreationTime != null && new Duration(tableCreationTime).isGreaterThan(duration);
    }

}
