package de.invesdwin.context.persistence.leveldb.timeseries.storage;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FDates;
import ezdb.serde.Serde;

@Immutable
public final class ShiftUnitsRangeKeySerde implements Serde<ShiftUnitsRangeKey> {

    public static final ShiftUnitsRangeKeySerde GET = new ShiftUnitsRangeKeySerde();
    public static final int FIXED_LENGTH = 8 + 4;

    private ShiftUnitsRangeKeySerde() {}

    @Override
    public ShiftUnitsRangeKey fromBytes(final byte[] bytes) {
        final ByteBuffer buf = ByteBuffer.wrap(bytes);
        final int shiftUnits = buf.getInt();
        final FDate rangeKey = FDates.extractFDate(buf);
        return new ShiftUnitsRangeKey(shiftUnits, rangeKey);
    }

    @Override
    public byte[] toBytes(final ShiftUnitsRangeKey obj) {
        final ByteBuffer buf = ByteBuffer.allocate(FIXED_LENGTH);
        buf.putInt(obj.getShiftUnits());
        FDates.putFDate(buf, obj.getRangeKey());
        return buf.array();
    }

}
