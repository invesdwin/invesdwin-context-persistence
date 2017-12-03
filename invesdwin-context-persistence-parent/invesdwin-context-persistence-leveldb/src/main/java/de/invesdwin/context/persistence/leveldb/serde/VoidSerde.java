package de.invesdwin.context.persistence.leveldb.serde;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.math.Bytes;
import ezdb.serde.Serde;

// TODO: move this class into ezdb itself sometime
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
