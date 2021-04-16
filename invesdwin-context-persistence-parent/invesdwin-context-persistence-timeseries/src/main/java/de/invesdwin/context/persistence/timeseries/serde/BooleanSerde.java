package de.invesdwin.context.persistence.timeseries.serde;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.math.Booleans;
import ezdb.serde.Serde;

@Immutable
public class BooleanSerde implements Serde<Boolean> {

    public static final BooleanSerde GET = new BooleanSerde();
    public static final int FIXED_LENGTH = Booleans.BYTES;

    @Override
    public Boolean fromBytes(final byte[] bytes) {
        final ByteBuffer buf = ByteBuffer.wrap(bytes);
        return Booleans.extractBoolean(buf);
    }

    @Override
    public byte[] toBytes(final Boolean obj) {
        final ByteBuffer buf = ByteBuffer.allocate(FIXED_LENGTH);
        Booleans.putBoolean(buf, obj);
        return buf.array();
    }

}
