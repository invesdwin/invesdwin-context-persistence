package de.invesdwin.context.persistence.timeseries.reference;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class HardReference<T> implements IPersistentReference<T> {

    private T referent;

    public HardReference(final T referent) {
        this.referent = referent;
    }

    @Override
    public T get() {
        return referent;
    }

    @Override
    public void clear() {
        //noop
    }

    @Override
    public void close() {
        referent = null;
    }

}
