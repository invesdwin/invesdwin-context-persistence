package de.invesdwin.context.persistence.leveldb.timeseries.storage;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.math.Bytes;
import ezdb.serde.Serde;

@Immutable
public final class SingleValueSerde implements Serde<SingleValue> {

    public static final SingleValueSerde GET = new SingleValueSerde();

    private SingleValueSerde() {}

    @Override
    public SingleValue fromBytes(final byte[] bytes) {
        if (bytes.length == 0) {
            return null;
        }
        return new SingleValue(bytes);
    }

    @Override
    public byte[] toBytes(final SingleValue obj) {
        if (obj == null) {
            return Bytes.EMPTY_ARRAY;
        }
        return obj.getBytes();
    }

}
