package de.invesdwin.context.persistence.timeseries.timeseriesdb.storage;

import javax.annotation.concurrent.Immutable;

import ezdb.serde.Serde;

@Immutable
public class SingleValue {

    private final byte[] bytes;
    private final Object value;

    public <V> SingleValue(final Serde<V> serde, final V value) {
        this.bytes = serde.toBytes(value);
        this.value = value;
    }

    public SingleValue(final byte[] bytes) {
        this.bytes = bytes;
        this.value = null;
    }

    @SuppressWarnings("unchecked")
    public <V> V getValue(final Serde<V> serde) {
        if (value == null) {
            return serde.fromBytes(bytes);
        } else {
            return (V) value;
        }
    }

    public byte[] getBytes() {
        return bytes;
    }

}
