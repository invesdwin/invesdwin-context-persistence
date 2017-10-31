package de.invesdwin.context.persistence.leveldb.serde;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.math.decimal.Decimal;
import ezdb.serde.Serde;

@Immutable
public final class DecimalSerde implements Serde<Decimal> {

    public static final DecimalSerde GET = new DecimalSerde();
    public static final Integer FIXED_LENGTH = 8;

    private DecimalSerde() {}

    @Override
    public Decimal fromBytes(final byte[] bytes) {
        final ByteBuffer buf = ByteBuffer.wrap(bytes);
        return Decimal.extractDecimal(buf);
    }

    @Override
    public byte[] toBytes(final Decimal obj) {
        final ByteBuffer buf = ByteBuffer.allocate(FIXED_LENGTH);
        Decimal.putDecimal(buf, obj);
        return buf.array();
    }

}
