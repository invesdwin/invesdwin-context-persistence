package de.invesdwin.context.persistence.leveldb.reference;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.streams.LZ4Streams;
import de.invesdwin.context.persistence.leveldb.serde.CompressingDelegateSerde;
import ezdb.serde.Serde;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;

/**
 * Behaves just like a WeakReference, with the distinction that the value is not discarded, but instead serialized until
 * it is requested again.
 * 
 * Thus this reference will never return null if the referent was not null in the first place.
 * 
 * <a href="http://stackoverflow.com/questions/10878012/using-referencequeue-and-WeakReference">Source</a>
 */
@ThreadSafe
public class SerdeCompressingWeakReference<T> extends ACompressingWeakReference<T, byte[]> {

    private final Serde<T> compressingSerde;

    public SerdeCompressingWeakReference(final T referent, final Serde<T> serde) {
        super(referent);
        this.compressingSerde = new CompressingDelegateSerde<T>(serde) {
            @Override
            protected LZ4BlockOutputStream newCompressor(final OutputStream out) {
                return SerdeCompressingWeakReference.this.newCompressor(out);
            }

            @Override
            protected LZ4BlockInputStream newDecompressor(final ByteArrayInputStream bis) {
                return SerdeCompressingWeakReference.this.newDecompressor(bis);
            }
        };
    }

    @Override
    protected T fromCompressed(final byte[] compressed) throws Exception {
        return compressingSerde.fromBytes(compressed);
    }

    @Override
    protected byte[] toCompressed(final T referent) throws Exception {
        return compressingSerde.toBytes(referent);
    }

    protected LZ4BlockOutputStream newCompressor(final OutputStream out) {
        return LZ4Streams.newDefaultLZ4OutputStream(out);
    }

    protected LZ4BlockInputStream newDecompressor(final ByteArrayInputStream bis) {
        return LZ4Streams.newDefaultLZ4InputStream(bis);
    }

}
