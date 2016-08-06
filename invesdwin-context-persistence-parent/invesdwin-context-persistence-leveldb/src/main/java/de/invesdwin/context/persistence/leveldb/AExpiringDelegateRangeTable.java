package de.invesdwin.context.persistence.leveldb;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FDate;

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
