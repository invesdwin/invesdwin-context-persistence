package de.invesdwin.context.persistence.timeseries.serde;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.math.Integers;
import ezdb.serde.Serde;

@Immutable
public class IntegerSerde implements Serde<Integer> {

    public static final IntegerSerde GET = new IntegerSerde();
    public static final int FIXED_LENGTH = Integer.BYTES;

    @Override
    public Integer fromBytes(final byte[] bytes) {
        final ByteBuffer buf = ByteBuffer.wrap(bytes);
        return Integers.extractInteger(buf);
    }

    @Override
    public byte[] toBytes(final Integer obj) {
        final ByteBuffer buf = ByteBuffer.allocate(FIXED_LENGTH);
        Integers.putInteger(buf, obj);
        return buf.array();
    }

}
