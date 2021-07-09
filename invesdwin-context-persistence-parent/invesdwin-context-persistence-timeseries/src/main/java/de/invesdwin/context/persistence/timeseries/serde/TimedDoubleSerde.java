package de.invesdwin.context.persistence.timeseries.serde;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.math.TimedDouble;
import de.invesdwin.util.time.date.FDate;
import ezdb.serde.Serde;

@ThreadSafe
public class TimedDoubleSerde implements Serde<TimedDouble> {

    public static final TimedDoubleSerde GET = new TimedDoubleSerde();
    public static final Integer FIXED_LENGTH = 8 + 8;
    public static final FixedLengthBufferingIteratorDelegateSerde<TimedDouble> GET_LIST = new FixedLengthBufferingIteratorDelegateSerde<TimedDouble>(
            GET, FIXED_LENGTH);

    static {
        Assertions.assertThat(GET.fromBytes(GET.toBytes(TimedDouble.DUMMY))).isEqualTo(TimedDouble.DUMMY);
    }

    @Override
    public TimedDouble fromBytes(final byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        final long time = buffer.getLong();
        final double value = buffer.getDouble();

        final TimedDouble dto = new TimedDouble(FDate.valueOf(time), value);
        return dto;
    }

    @Override
    public byte[] toBytes(final TimedDouble obj) {
        if (obj == null) {
            return Bytes.EMPTY_ARRAY;
        }

        final long time = obj.getTime().millisValue();
        final double value = obj.getValue();

        final ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(time);
        buffer.putDouble(value);
        return buffer.array();
    }

}
