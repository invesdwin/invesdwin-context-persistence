package de.invesdwin.context.persistence.leveldb.reference;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.IOUtils;

import de.invesdwin.context.persistence.leveldb.timeseries.SerializingCollection;
import ezdb.serde.Serde;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;

/**
 * Behaves just like a SoftReference, with the distinction that the value is not discarded, but instead serialized until
 * it is requested again.
 * 
 * Thus this reference will never return null if the referent was not null in the first place.
 * 
 * <a href="http://stackoverflow.com/questions/10878012/using-referencequeue-and-SoftReference">Source</a>
 */
@ThreadSafe
public class SerdeCompressingSoftReference<T> extends ACompressingSoftReference<T, byte[]> {

    private final Serde<T> serde;

    public SerdeCompressingSoftReference(final T referent, final Serde<T> serde) {
        super(referent);
        this.serde = serde;
    }

    @Override
    protected T fromCompressed(final byte[] compressed) throws Exception {
        final ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
        final InputStream in = newDecompressor(bis);
        final byte[] bytes = IOUtils.toByteArray(in);
        in.close();
        return serde.fromBytes(bytes);
    }

    @Override
    protected byte[] toCompressed(final T referent) throws Exception {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final OutputStream out = newCompressor(bos);
        out.write(serde.toBytes(referent));
        out.close();
        return bos.toByteArray();
    }

    protected LZ4BlockOutputStream newCompressor(final OutputStream out) {
        return SerializingCollection.newDefaultLZ4BlockOutputStream(out);
    }

    protected LZ4BlockInputStream newDecompressor(final ByteArrayInputStream bis) {
        return new LZ4BlockInputStream(bis, LZ4Factory.fastestInstance().fastDecompressor());
    }

}
