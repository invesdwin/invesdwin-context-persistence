package de.invesdwin.context.persistence.timeseries.timeseriesdb.storage;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.serde.ISerde;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;

@Immutable
public final class ShiftUnitsRangeKeySerde implements ISerde<ShiftUnitsRangeKey> {

    public static final ShiftUnitsRangeKeySerde GET = new ShiftUnitsRangeKeySerde();
    public static final int FIXED_LENGTH = 8 + 4;

    private ShiftUnitsRangeKeySerde() {
    }

    @Override
    public ShiftUnitsRangeKey fromBytes(final byte[] bytes) {
        final ByteBuffer buf = ByteBuffer.wrap(bytes);
        final FDate rangeKey = FDates.extractFDate(buf);
        final int shiftUnits = buf.getInt();
        return new ShiftUnitsRangeKey(rangeKey, shiftUnits);
    }

    @Override
    public byte[] toBytes(final ShiftUnitsRangeKey obj) {
        final ByteBuffer buf = ByteBuffer.allocate(FIXED_LENGTH);
        FDates.putFDate(buf, obj.getRangeKey());
        buf.putInt(obj.getShiftUnits());
        return buf.array();
    }

}
