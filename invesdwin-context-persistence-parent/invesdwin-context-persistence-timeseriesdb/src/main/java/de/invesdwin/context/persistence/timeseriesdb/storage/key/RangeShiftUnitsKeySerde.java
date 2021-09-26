package de.invesdwin.context.persistence.timeseriesdb.storage.key;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.SerdeBaseMethods;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.time.date.FDate;

@Immutable
public final class RangeShiftUnitsKeySerde implements ISerde<RangeShiftUnitsKey> {

    public static final RangeShiftUnitsKeySerde GET = new RangeShiftUnitsKeySerde();

    public static final int TIME_INDEX = 0;
    public static final int TIME_SIZE = FDate.BYTES;

    public static final int SHIFTUNITS_INDEX = TIME_INDEX + TIME_SIZE;
    public static final int SHIFTUNITS_SIZE = Integer.BYTES;

    public static final int FIXED_LENGTH = SHIFTUNITS_INDEX + SHIFTUNITS_SIZE;

    private RangeShiftUnitsKeySerde() {
    }

    @Override
    public RangeShiftUnitsKey fromBytes(final byte[] bytes) {
        return SerdeBaseMethods.fromBytes(this, bytes);
    }

    @Override
    public byte[] toBytes(final RangeShiftUnitsKey obj) {
        return SerdeBaseMethods.toBytes(this, obj);
    }

    @Override
    public RangeShiftUnitsKey fromBuffer(final IByteBuffer buffer, final int length) {
        final FDate rangeKey = FDateSerde.getFDate(buffer, TIME_INDEX);
        final int shiftUnits = buffer.getInt(SHIFTUNITS_INDEX);
        return new RangeShiftUnitsKey(rangeKey, shiftUnits);
    }

    @Override
    public int toBuffer(final IByteBuffer buffer, final RangeShiftUnitsKey obj) {
        FDateSerde.putFDate(buffer, TIME_INDEX, obj.getRangeKey());
        buffer.putInt(SHIFTUNITS_INDEX, obj.getShiftUnits());
        return FIXED_LENGTH;
    }

}
