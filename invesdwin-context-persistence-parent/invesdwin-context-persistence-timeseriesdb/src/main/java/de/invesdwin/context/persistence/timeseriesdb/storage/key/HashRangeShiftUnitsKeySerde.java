package de.invesdwin.context.persistence.timeseriesdb.storage.key;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.SerdeBaseMethods;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.time.date.FDate;

@Immutable
public final class HashRangeShiftUnitsKeySerde implements ISerde<HashRangeShiftUnitsKey> {

    public static final HashRangeShiftUnitsKeySerde GET = new HashRangeShiftUnitsKeySerde();

    private static final int RANGEKEY_INDEX = 0;
    private static final int RANGEKEY_SIZE = FDate.BYTES;

    public static final int SHIFTUNITS_INDEX = RANGEKEY_INDEX + RANGEKEY_SIZE;
    private static final int SHIFTUNITS_SIZE = Integer.BYTES;

    private static final int HASHKEY_INDEX = SHIFTUNITS_INDEX + SHIFTUNITS_SIZE;

    private HashRangeShiftUnitsKeySerde() {}

    @Override
    public HashRangeShiftUnitsKey fromBytes(final byte[] bytes) {
        return SerdeBaseMethods.fromBytes(this, bytes);
    }

    @Override
    public byte[] toBytes(final HashRangeShiftUnitsKey obj) {
        return SerdeBaseMethods.toBytes(this, obj);
    }

    @Override
    public HashRangeShiftUnitsKey fromBuffer(final IByteBuffer buffer) {
        final FDate rangeKey = FDateSerde.getFDate(buffer, RANGEKEY_INDEX);
        final int shiftUnits = buffer.getInt(SHIFTUNITS_INDEX);
        final String hashKey = buffer.getStringUtf8(HASHKEY_INDEX, buffer.capacity() - HASHKEY_INDEX);
        return new HashRangeShiftUnitsKey(hashKey, rangeKey, shiftUnits);
    }

    @Override
    public int toBuffer(final IByteBuffer buffer, final HashRangeShiftUnitsKey obj) {
        FDateSerde.putFDate(buffer, RANGEKEY_INDEX, obj.getRangeKey());
        buffer.putInt(SHIFTUNITS_INDEX, obj.getShiftUnits());
        final int length = buffer.putStringUtf8(HASHKEY_INDEX, obj.getHashKey());
        return HASHKEY_INDEX + length;
    }

}
