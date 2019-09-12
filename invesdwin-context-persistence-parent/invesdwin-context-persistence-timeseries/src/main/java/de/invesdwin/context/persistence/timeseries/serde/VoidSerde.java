package de.invesdwin.context.persistence.timeseries.serde;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.math.Bytes;
import ezdb.serde.Serde;

@Immutable
public class VoidSerde implements Serde<Void> {

    public static final VoidSerde GET = new VoidSerde();

    @Override
    public Void fromBytes(final byte[] bytes) {
        return null;
    }

    @Override
    public byte[] toBytes(final Void obj) {
        return Bytes.EMPTY_ARRAY;
    }

}
