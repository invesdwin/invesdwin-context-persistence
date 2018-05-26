package de.invesdwin.context.persistence.leveldb.timeseries.segmented;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.time.TimeRange;

@Immutable
public class SegmentedKey<K> {

    private final K key;
    private final TimeRange segment;

    public SegmentedKey(final K key, final TimeRange segment) {
        this.key = key;
        this.segment = segment;
    }

    public K getKey() {
        return key;
    }

    public TimeRange getSegment() {
        return segment;
    }

}
