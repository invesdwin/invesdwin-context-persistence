package de.invesdwin.context.persistence.timeseries.timeseriesdb.storage;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.norva.marker.ISerializableValueObject;
import ezdb.serde.Serde;

@Immutable
public class ChunkValue implements ISerializableValueObject {

    private final byte[] firstValue;
    private final byte[] lastValue;
    private final int count;

    public <V> ChunkValue(final byte[] firstValue, final byte[] lastValue, final int count) {
        this.firstValue = firstValue;
        this.lastValue = lastValue;
        this.count = count;
    }

    public <V> ChunkValue(final Serde<V> serde, final V firstValue, final V lastValue, final int count) {
        this.firstValue = serde.toBytes(firstValue);
        this.lastValue = serde.toBytes(lastValue);
        this.count = count;
    }

    public <V> V getFirstValue(final Serde<V> serde) {
        return serde.fromBytes(firstValue);
    }

    public byte[] getFirstValue() {
        return firstValue;
    }

    public <V> V getLastValue(final Serde<V> serde) {
        return serde.fromBytes(lastValue);
    }

    public byte[] getLastValue() {
        return lastValue;
    }

    public int getCount() {
        return count;
    }
}