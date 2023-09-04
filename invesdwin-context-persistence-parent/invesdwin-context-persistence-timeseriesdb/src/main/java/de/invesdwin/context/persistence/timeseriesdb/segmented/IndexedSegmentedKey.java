package de.invesdwin.context.persistence.timeseriesdb.segmented;

import javax.annotation.concurrent.Immutable;

@Immutable
public class IndexedSegmentedKey<K> {

    private final SegmentedKey<K> segmentedKey;
    private final long precedingValueCount;

    public IndexedSegmentedKey(final SegmentedKey<K> segmentedKey, final long precedingValueCount) {
        this.segmentedKey = segmentedKey;
        this.precedingValueCount = precedingValueCount;
    }

    public SegmentedKey<K> getSegmentedKey() {
        return segmentedKey;
    }

    public long getPrecedingValueCount() {
        return precedingValueCount;
    }

}
