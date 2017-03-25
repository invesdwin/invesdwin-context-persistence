package de.invesdwin.context.persistence.leveldb.reference;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;

/**
 * Behaves just like a SoftReference, with the distinction that the value is not discarded, but instead serialized until
 * it is requested again.
 * 
 * Thus this reference will never return null if the referent was not null in the first place.
 * 
 * <a href="http://stackoverflow.com/questions/10878012/using-referencequeue-and-SoftReference">Source</a>
 */
@ThreadSafe
public abstract class ACompressingSoftReference<T, C> extends SoftReference<T> implements IPersistentReference<T> {

    private static final ReferenceQueue<Object> REAPED_QUEUE = new ReferenceQueue<Object>();
    private DelegateSoftReference<T> delegate;
    private C compressed;

    static {
        final WrappedExecutorService executor = Executors
                .newFixedCallerRunsThreadPool(ACompressingSoftReference.class.getSimpleName(), 1);
        executor.execute(new Runnable() {
            @SuppressWarnings("unchecked")
            @Override
            public void run() {
                try {
                    while (true) {
                        final DelegateSoftReference<? extends Object> removed = (DelegateSoftReference<? extends Object>) REAPED_QUEUE
                                .remove();
                        removed.clear();
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    public ACompressingSoftReference(final T referent) {
        super(null);
        this.delegate = new DelegateSoftReference<T>(this, referent);
    }

    @Override
    public synchronized T get() {
        if (delegate == null) {
            if (compressed == null) {
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
                if (compressed == null) {
                    compressed = toCompressed(referent);
                }
                delegate = null;
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected abstract C toCompressed(final T referent) throws Exception;

    protected abstract T fromCompressed(C compressed) throws Exception;

    private void readReferent() {
        try {
            final T referent = fromCompressed(compressed);
            delegate = new DelegateSoftReference<T>(this, referent);
            if (!keepCompressedData()) {
                compressed = null;
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected boolean keepCompressedData() {
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
        compressed = null;
    }

    private static class DelegateSoftReference<T> extends SoftReference<WrappedReferent<T>> {

        private final ACompressingSoftReference<T, ?> parent;
        private final T hardReferent;

        DelegateSoftReference(final ACompressingSoftReference<T, ?> parent, final T referent) {
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
