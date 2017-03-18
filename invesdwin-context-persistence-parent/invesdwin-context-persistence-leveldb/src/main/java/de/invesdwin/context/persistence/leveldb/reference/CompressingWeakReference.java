package de.invesdwin.context.persistence.leveldb.reference;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.IOUtils;

import de.invesdwin.context.persistence.leveldb.timeseries.SerializingCollection;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import ezdb.serde.Serde;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;

/**
 * Behaves just like a WeakReference, with the distinction that the value is not discarded, but instead serialized until
 * it is requested again.
 * 
 * Thus this reference will never return null if the referent was not null in the first place.
 * 
 * <a href="http://stackoverflow.com/questions/10878012/using-referencequeue-and-weakreference">Source</a>
 */
@ThreadSafe
public class CompressingWeakReference<T> extends WeakReference<T> implements IPersistentReference<T> {

    private static final ReferenceQueue<Object> REAPED_QUEUE = new ReferenceQueue<Object>();
    @GuardedBy("this")
    private DelegateWeakReference<T> delegate;
    @GuardedBy("this")
    private byte[] compressedBytes;
    private final Serde<T> serde;

    static {
        final WrappedExecutorService executor = Executors
                .newFixedCallerRunsThreadPool(CompressingWeakReference.class.getSimpleName(), 1);
        executor.execute(new Runnable() {
            @SuppressWarnings("unchecked")
            @Override
            public void run() {
                try {
                    while (true) {
                        final DelegateWeakReference<? extends Object> removed = (DelegateWeakReference<? extends Object>) REAPED_QUEUE
                                .remove();
                        removed.clear();
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    public CompressingWeakReference(final T referent, final Serde<T> serde) {
        super(null);
        this.delegate = new DelegateWeakReference<T>(this, referent);
        this.serde = serde;
    }

    @Override
    public synchronized T get() {
        if (delegate == null) {
            if (compressedBytes == null) {
                return null;
            }
            readReferent();
        }
        return delegate.hardReferent;
    }

    private synchronized void writeReferent() {
        if (delegate == null) {
            //already closed
            return;
        }
        final T referent = delegate.hardReferent;
        if (referent != null) {
            try {
                if (compressedBytes == null) {
                    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    final OutputStream out = newCompressor(bos);
                    out.write(serde.toBytes(referent));
                    out.close();
                    compressedBytes = bos.toByteArray();
                }
                delegate = null;
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected LZ4BlockOutputStream newCompressor(final OutputStream out) {
        return SerializingCollection.newDefaultLZ4BlockOutputStream(out);
    }

    protected LZ4BlockInputStream newDecompressor(final ByteArrayInputStream bis) {
        return new LZ4BlockInputStream(bis, LZ4Factory.fastestInstance().fastDecompressor());
    }

    private void readReferent() {
        try {
            final ByteArrayInputStream bis = new ByteArrayInputStream(compressedBytes);
            final InputStream in = newDecompressor(bis);
            final byte[] bytes = IOUtils.toByteArray(in);
            in.close();
            final T referent = serde.fromBytes(bytes);
            delegate = new DelegateWeakReference<T>(this, referent);
            if (!keepCompressedBytes()) {
                compressedBytes = null;
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected boolean keepCompressedBytes() {
        return false;
    }

    /**
     * This can be used to manually serialize the object.
     */
    @Override
    public synchronized void clear() {
        if (delegate != null) {
            delegate.clear();
        }
        super.clear();
    }

    /**
     * Discards this reference and deletes the serialized file if it exists.
     */
    @Override
    public synchronized void close() {
        delegate = null;
        compressedBytes = null;
    }

    private static class DelegateWeakReference<T> extends WeakReference<WrappedReferent<T>> {

        private final CompressingWeakReference<T> parent;
        private final T hardReferent;

        DelegateWeakReference(final CompressingWeakReference<T> parent, final T referent) {
            super(new WrappedReferent<T>(referent), REAPED_QUEUE);
            this.parent = parent;
            this.hardReferent = referent;
        }

        @Override
        public void clear() {
            parent.writeReferent();
            super.clear();
        }

    }

    private static class WrappedReferent<T> {
        @SuppressWarnings("unused")
        private final T referent;

        WrappedReferent(final T referent) {
            this.referent = referent;
        }

    }

}
