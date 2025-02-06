package de.invesdwin.context.persistence.timeseriesdb.delegate;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.ITimeSeriesDB;

@ThreadSafe
public class DelegateTimeSeriesDB<K, V> extends ADelegateTimeSeriesDB<K, V> {

    public DelegateTimeSeriesDB(final ITimeSeriesDB<K, V> delegate) {
        super(delegate);
    }

    @Deprecated
    @Override
    protected ITimeSeriesDB<K, V> newDelegate() {
        throw new UnsupportedOperationException();
    }

}