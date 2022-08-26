package de.invesdwin.context.persistence.timeseriesdb.storage.key;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.SerdeBaseMethods;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.time.date.FDate;

@Immutable
public final class HashRangeKeySerde implements ISerde<HashRangeKey> {

    public static final HashRangeKeySerde GET = new HashRangeKeySerde();

    private static final int RANGEKEY_INDEX = 0;
    private static final int RANGEKEY_SIZE = FDate.BYTES;

    private static final int HASHKEY_INDEX = RANGEKEY_INDEX + RANGEKEY_SIZE;

    private HashRangeKeySerde() {}

    @Override
    public HashRangeKey fromBytes(final byte[] bytes) {
        return SerdeBaseMethods.fromBytes(this, bytes);
    }

    @Override
    public byte[] toBytes(final HashRangeKey obj) {
        return SerdeBaseMethods.toBytes(this, obj);
    }

    @Override
    public HashRangeKey fromBuffer(final IByteBuffer buffer) {
        final FDate rangeKey = FDateSerde.getFDate(buffer, RANGEKEY_INDEX);
        final String hashKey = buffer.getStringUtf8(HASHKEY_INDEX, buffer.capacity() - HASHKEY_INDEX);
        return new HashRangeKey(hashKey, rangeKey);
    }

    @Override
    public int toBuffer(final IByteBuffer buffer, final HashRangeKey obj) {
        FDateSerde.putFDate(buffer, RANGEKEY_INDEX, obj.getRangeKey());
        final int length = buffer.putStringUtf8(HASHKEY_INDEX, obj.getHashKey());
        return HASHKEY_INDEX + length;
    }

}
