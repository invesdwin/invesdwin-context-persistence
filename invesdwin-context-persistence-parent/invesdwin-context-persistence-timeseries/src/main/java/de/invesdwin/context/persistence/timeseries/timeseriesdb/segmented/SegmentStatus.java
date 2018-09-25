package de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented;

import javax.annotation.concurrent.Immutable;

@Immutable
public enum SegmentStatus {
    INITIALIZING,
    COMPLETE,
    COMPLETE_EMPTY;
}
