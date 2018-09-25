package de.invesdwin.context.persistence.timeseries.serde;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FDates;
import ezdb.serde.Serde;

@Immutable
public class FDateSerde implements Serde<FDate> {

    public static final FDateSerde GET = new FDateSerde();
    public static final int FIXED_LENGTH = 8;

    @Override
    public FDate fromBytes(final byte[] bytes) {
        final ByteBuffer buf = ByteBuffer.wrap(bytes);
        return FDates.extractFDate(buf);
    }

    @Override
    public byte[] toBytes(final FDate obj) {
        final ByteBuffer buf = ByteBuffer.allocate(FIXED_LENGTH);
        FDates.putFDate(buf, obj);
        return buf.array();
    }

}
