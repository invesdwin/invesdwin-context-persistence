package de.invesdwin.context.persistence.timeseries.serde;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.math.decimal.Decimal;
import de.invesdwin.util.math.decimal.TimedDecimal;
import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FDates;
import ezdb.serde.Serde;

@Immutable
public class TimedDecimalSerde implements Serde<TimedDecimal> {

    public static final TimedDecimalSerde GET = new TimedDecimalSerde();
    public static final Integer FIXED_LENGTH = 8 + 8;

    public TimedDecimalSerde() {}

    @Override
    public TimedDecimal fromBytes(final byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        final FDate time = FDates.extractFDate(buffer);
        final Decimal percent = Decimal.extractDecimal(buffer);

        final TimedDecimal timedMoney = new TimedDecimal(time, percent);
        return timedMoney;
    }

    @Override
    public byte[] toBytes(final TimedDecimal obj) {
        final ByteBuffer buffer = ByteBuffer.allocate(16);
        FDates.putFDate(buffer, obj.getTime());
        Decimal.putDecimal(buffer, obj);
        return buffer.array();
    }

}
