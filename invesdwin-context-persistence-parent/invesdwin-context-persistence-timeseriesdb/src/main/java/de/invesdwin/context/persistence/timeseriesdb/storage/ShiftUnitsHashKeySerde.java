package de.invesdwin.context.persistence.timeseriesdb.storage;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.SerdeBaseMethods;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.time.date.FDate;

@Immutable
public final class ShiftUnitsHashKeySerde implements ISerde<ShiftUnitsHashKey> {

    public static final ShiftUnitsHashKeySerde GET = new ShiftUnitsHashKeySerde();

    private static final int RANGEKEY_INDEX = 0;
    private static final int RANGEKEY_SIZE = FDate.BYTES;

    public static final int SHIFTUNITS_INDEX = RANGEKEY_INDEX + RANGEKEY_SIZE;
    private static final int SHIFTUNITS_SIZE = Integer.BYTES;

    private static final int HASHKEY_INDEX = SHIFTUNITS_INDEX + SHIFTUNITS_SIZE;

    private ShiftUnitsHashKeySerde() {
    }

    @Override
    public ShiftUnitsHashKey fromBytes(final byte[] bytes) {
        return SerdeBaseMethods.fromBytes(this, bytes);
    }

    @Override
    public byte[] toBytes(final ShiftUnitsHashKey obj) {
        return SerdeBaseMethods.toBytes(this, obj);
    }

    @Override
    public ShiftUnitsHashKey fromBuffer(final IByteBuffer buffer, final int length) {
        final FDate rangeKey = FDateSerde.getFDate(buffer, RANGEKEY_INDEX);
        final int shiftUnits = buffer.getInt(SHIFTUNITS_INDEX);
        final String hashKey = buffer.getStringUtf8(shiftUnits, length - HASHKEY_INDEX);
        return new ShiftUnitsHashKey(hashKey, rangeKey, shiftUnits);
    }

    @Override
    public int toBuffer(final IByteBuffer buffer, final ShiftUnitsHashKey obj) {
        FDateSerde.putFDate(buffer, RANGEKEY_INDEX, obj.getRangeKey());
        buffer.putInt(SHIFTUNITS_INDEX, obj.getShiftUnits());
        final int length = buffer.putStringUtf8(HASHKEY_INDEX, obj.getHashKey());
        return HASHKEY_INDEX + length;
    }

}
