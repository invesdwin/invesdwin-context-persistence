package de.invesdwin.context.persistence.leveldb.timeseries.segmented;

import javax.annotation.concurrent.Immutable;

@Immutable
public enum SegmentStatus {
    INITIALIZING,
    COMPLETE;
}
