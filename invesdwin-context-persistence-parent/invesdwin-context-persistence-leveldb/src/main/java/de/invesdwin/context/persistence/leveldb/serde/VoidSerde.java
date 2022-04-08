package de.invesdwin.context.persistence.leveldb.serde;

import javax.annotation.concurrent.Immutable;

import ezdb.serde.Serde;

// TODO: move this class into ezdb itself sometime
@Immutable
public class VoidSerde implements Serde<Void> {

    //CHECKSTYLE:OFF
    public static final VoidSerde get = new VoidSerde();
    //CHECKSTYLE:ON
    private static final byte[] EMPTY_BYTES = new byte[0];

    @Override
    public Void fromBytes(final byte[] bytes) {
        return null;
    }

    @Override
    public byte[] toBytes(final Void obj) {
        return EMPTY_BYTES;
    }

}
