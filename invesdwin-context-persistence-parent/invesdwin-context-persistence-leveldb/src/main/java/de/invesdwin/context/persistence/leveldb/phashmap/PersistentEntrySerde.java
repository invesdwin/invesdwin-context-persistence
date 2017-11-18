package de.invesdwin.context.persistence.leveldb.phashmap;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.math.Booleans;
import de.invesdwin.util.math.Bytes;
import ezdb.serde.Serde;

@Immutable
public final class PersistentEntrySerde implements Serde<PersistentEntry> {

    public static final byte[] MISSING_VALUE = new byte[0];
    public static final PersistentEntrySerde GET = new PersistentEntrySerde();

    private static final int ADDITIONAL_SIZE = Byte.BYTES;

    private PersistentEntrySerde() {}

    @Override
    public PersistentEntry fromBytes(final byte[] bytes) {
        if (bytes.length == 0) {
            return null;
        }
        final ByteBuffer buf = ByteBuffer.wrap(bytes);
        final boolean hashKey = Booleans.checkedCast(buf.get());
        final byte[] value = new byte[buf.remaining()];
        buf.get(value);
        return new PersistentEntry(hashKey, value);
    }

    @Override
    public byte[] toBytes(final PersistentEntry obj) {
        if (obj == null) {
            return MISSING_VALUE;
        }
        final boolean key = obj.isKey();
        final byte[] value = obj.getEntry();
        final ByteBuffer buf = ByteBuffer.allocate(value.length + ADDITIONAL_SIZE);
        buf.put(Bytes.checkedCast(key));
        buf.put(value);
        return buf.array();
    }

}
