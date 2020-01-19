package de.invesdwin.context.persistence.timeseries.timeseriesdb.storage;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.norva.marker.ISerializableValueObject;
import ezdb.serde.Serde;

@Immutable
public class ChunkValue implements ISerializableValueObject {

    private final byte[] firstValue;
    private final byte[] lastValue;
    private final int count;

    public <V> ChunkValue(final Serde<V> serde, final V firstValue, final V lastValue, final int count) {
        this.firstValue = serde.toBytes(firstValue);
        this.lastValue = serde.toBytes(lastValue);
        this.count = count;
    }

    public <V> V getFirstValue(final Serde<V> serde) {
        return serde.fromBytes(firstValue);
    }

    public <V> V getLastValue(final Serde<V> serde) {
        return serde.fromBytes(lastValue);
    }

    public int getCount() {
        return count;
    }
}