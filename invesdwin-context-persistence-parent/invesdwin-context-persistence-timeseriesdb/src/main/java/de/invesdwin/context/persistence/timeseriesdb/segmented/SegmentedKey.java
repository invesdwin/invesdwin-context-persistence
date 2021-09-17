package de.invesdwin.context.persistence.timeseriesdb.segmented;

import javax.annotation.concurrent.Immutable;
import javax.persistence.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;

import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.bean.AValueObject;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.time.range.TimeRange;

@Immutable
public class SegmentedKey<K> extends AValueObject {

    private final K key;
    private final TimeRange segment;
    @Transient
    @JsonIgnore
    private transient Integer cachedHashCode;

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

    @Override
    public int hashCode() {
        if (cachedHashCode == null) {
            cachedHashCode = Objects.hashCode(SegmentedKey.class, key, segment);
        }
        return cachedHashCode;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof SegmentedKey) {
            @SuppressWarnings("unchecked")
            final SegmentedKey<K> cObj = (SegmentedKey<K>) obj;
            return Objects.equals(key, cObj.key) && Objects.equals(segment, cObj.segment);
        } else {
            return false;
        }
    }

}
