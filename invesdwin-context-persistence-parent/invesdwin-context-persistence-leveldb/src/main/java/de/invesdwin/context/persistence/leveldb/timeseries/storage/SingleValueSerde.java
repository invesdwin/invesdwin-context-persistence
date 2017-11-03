package de.invesdwin.context.persistence.leveldb.timeseries.storage;

import javax.annotation.concurrent.Immutable;

import ezdb.serde.Serde;

@Immutable
public final class SingleValueSerde implements Serde<SingleValue> {

    public static final SingleValueSerde GET = new SingleValueSerde();

    private SingleValueSerde() {}

    @Override
    public SingleValue fromBytes(final byte[] bytes) {
        return new SingleValue(bytes);
    }

    @Override
    public byte[] toBytes(final SingleValue obj) {
        return obj.getBytes();
    }

}
