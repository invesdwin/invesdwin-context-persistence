package de.invesdwin.context.persistence.timeseriesdb.storage;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.norva.marker.ISerializableValueObject;
import de.invesdwin.util.marshallers.serde.ISerde;

@Immutable
public class FileSummary implements ISerializableValueObject {

    private final byte[] firstValue;
    private final byte[] lastValue;
    private final int valueCount;
    private final long addressOffset;
    private final long addressSize;

    public <V> FileSummary(final byte[] firstValue, final byte[] lastValue, final int valueCount,
            final long addressOffset, final long addressSize) {
        this.firstValue = firstValue;
        this.lastValue = lastValue;
        this.valueCount = valueCount;
        this.addressOffset = addressOffset;
        this.addressSize = addressSize;
    }

    public <V> FileSummary(final ISerde<V> serde, final V firstValue, final V lastValue, final int valueCount,
            final long addressOffset, final long addressSize) {
        this.firstValue = serde.toBytes(firstValue);
        this.lastValue = serde.toBytes(lastValue);
        this.valueCount = valueCount;
        this.addressOffset = addressOffset;
        this.addressSize = addressSize;
    }

    public <V> V getFirstValue(final ISerde<V> serde) {
        return serde.fromBytes(firstValue);
    }

    public byte[] getFirstValue() {
        return firstValue;
    }

    public <V> V getLastValue(final ISerde<V> serde) {
        return serde.fromBytes(lastValue);
    }

    public byte[] getLastValue() {
        return lastValue;
    }

    public int getValueCount() {
        return valueCount;
    }

    public long getAddressOffset() {
        return addressOffset;
    }

    public long getAddressSize() {
        return addressSize;
    }
}