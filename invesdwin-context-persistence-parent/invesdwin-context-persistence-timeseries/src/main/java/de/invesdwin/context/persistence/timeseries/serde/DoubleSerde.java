package de.invesdwin.context.persistence.timeseries.serde;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.math.Doubles;
import ezdb.serde.Serde;

@Immutable
public final class DoubleSerde implements Serde<Double> {

    public static final DoubleSerde GET = new DoubleSerde();
    public static final Integer FIXED_LENGTH = 8;

    private DoubleSerde() {}

    @Override
    public Double fromBytes(final byte[] bytes) {
        final ByteBuffer buf = ByteBuffer.wrap(bytes);
        return Doubles.extractDouble(buf);
    }

    @Override
    public byte[] toBytes(final Double obj) {
        final ByteBuffer buf = ByteBuffer.allocate(FIXED_LENGTH);
        Doubles.putDouble(buf, obj);
        return buf.array();
    }

}
