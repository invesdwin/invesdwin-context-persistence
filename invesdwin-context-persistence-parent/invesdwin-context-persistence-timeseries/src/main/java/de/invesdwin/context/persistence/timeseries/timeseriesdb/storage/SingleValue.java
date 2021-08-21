package de.invesdwin.context.persistence.timeseries.timeseriesdb.storage;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.serde.ISerde;

@Immutable
public class SingleValue {

    private final byte[] bytes;
    private final Object value;

    public <V> SingleValue(final ISerde<V> serde, final V value) {
        this.bytes = serde.toBytes(value);
        this.value = value;
    }

    public SingleValue(final byte[] bytes) {
        this.bytes = bytes;
        this.value = null;
    }

    @SuppressWarnings("unchecked")
    public <V> V getValue(final ISerde<V> serde) {
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
