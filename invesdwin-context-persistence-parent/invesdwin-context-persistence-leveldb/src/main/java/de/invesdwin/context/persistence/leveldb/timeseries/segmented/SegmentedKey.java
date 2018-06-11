package de.invesdwin.context.persistence.leveldb.timeseries.segmented;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.bean.AValueObject;
import de.invesdwin.util.time.range.TimeRange;

@Immutable
public class SegmentedKey<K> extends AValueObject {

    private final K key;
    private final TimeRange segment;

    public SegmentedKey(final K key, final TimeRange segment) {
        Assertions.checkNotNull(key);
        this.key = key;
        Assertions.checkNotNull(segment);
        Assertions.checkNotNull(segment.getFrom());
        Assertions.checkNotNull(segment.getTo());
        this.segment = segment;
    }

    public K getKey() {
        return key;
    }

    public TimeRange getSegment() {
        return segment;
    }

}
