package de.invesdwin.context.persistence.timeseries.ezdb;

import javax.annotation.concurrent.Immutable;

@Immutable
public enum RangeTablePersistenceMode {
    DISK_ONLY(true),
    MEMORY_ONLY(false),
    MEMORY_WRITE_THROUGH_DISK(true);

    private boolean disk;

    RangeTablePersistenceMode(final boolean disk) {
        this.disk = disk;
    }

    public boolean isDisk() {
        return disk;
    }
}
