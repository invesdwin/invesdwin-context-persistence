package de.invesdwin.context.persistence.timeseriesdb.storage.key;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.time.date.FDate;

@Immutable
public class HashRangeKey implements Comparable<Object> {

    private final String hashKey;
    private final FDate rangeKey;

    public HashRangeKey(final String hashKey, final FDate rangeKey) {
        this.hashKey = hashKey;
        this.rangeKey = rangeKey;
    }

    public String getHashKey() {
        return hashKey;
    }

    public FDate getRangeKey() {
        return rangeKey;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof HashRangeKey) {
            final HashRangeKey cObj = (HashRangeKey) obj;
            return Objects.equals(hashKey, cObj.hashKey) && Objects.equals(rangeKey, cObj.rangeKey);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(HashRangeKey.class, hashKey, rangeKey);
    }

    @Override
    public int compareTo(final Object o) {
        if (o instanceof HashRangeKey) {
            final HashRangeKey cO = (HashRangeKey) o;
            int compare = hashKey.compareTo(cO.hashKey);
            if (compare != 0) {
                return compare;
            }
            compare = rangeKey.compareToNotNullSafe(cO.rangeKey);
            return compare;
        } else {
            return 1;
        }
    }

    @Override
    public String toString() {
        return Objects.toString(this);
    }

}
