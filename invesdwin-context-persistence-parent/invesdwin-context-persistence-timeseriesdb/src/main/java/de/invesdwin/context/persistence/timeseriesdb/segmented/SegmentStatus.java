package de.invesdwin.context.persistence.timeseriesdb.segmented;

import javax.annotation.concurrent.Immutable;

@Immutable
public enum SegmentStatus {
    INITIALIZING(false),
    COMPLETE(true),
    COMPLETE_EMPTY(true);

    private final boolean complete;

    SegmentStatus(final boolean complete) {
        this.complete = complete;
    }

    public boolean isComplete() {
        return complete;
    }
}
