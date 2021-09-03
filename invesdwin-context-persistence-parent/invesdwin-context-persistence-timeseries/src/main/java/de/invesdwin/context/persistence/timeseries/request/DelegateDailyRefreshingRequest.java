package de.invesdwin.context.persistence.timeseries.request;

import java.util.concurrent.Callable;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.network.DailyDownloadCache;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.integration.retry.task.ARetryCallable;
import de.invesdwin.context.integration.retry.task.RetryOriginator;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.date.FDate;

@ThreadSafe
public class DelegateDailyRefreshingRequest<E> implements Callable<E> {

    @GuardedBy("this")
    private FDate lastRequestTime;
    @GuardedBy("this")
    private E lastResponse;
    private final Callable<E> delegate;

    public DelegateDailyRefreshingRequest(final Callable<E> delegate) {
        Assertions.assertThat(delegate).isNotInstanceOf(DelegateDailyRefreshingRequest.class);
        this.delegate = delegate;
    }

    @Override
    public E call() {
        final E updated = update();
        if (updated != null) {
            //onResponse should not be invoked with any locks
            onResponse(updated);
            return updated;
        } else {
            synchronized (this) {
                return lastResponse;
            }
        }
    }

    private synchronized E update() {
        if (lastResponse == null || !ContextProperties.IS_TEST_ENVIRONMENT && shouldUpdate(lastRequestTime)) {
            lastRequestTime = getNow();
            lastResponse = downloadWithRetry();
            return lastResponse;
        }
        return null;
    }

    protected boolean shouldUpdate(final FDate lastRequestTime) {
        return new DailyDownloadCache().shouldUpdate(lastRequestTime, getNow());
    }

    protected FDate getNow() {
        return new FDate();
    }

    protected void onResponse(final E response) {
    }

    public synchronized FDate getLastRequestTime() {
        return lastRequestTime;
    }

    //do not expect aspects to work in jforex
    private E downloadWithRetry() {
        try {
            return new ARetryCallable<E>(new RetryOriginator(this, "downloadWithRetry")) {
                @Override
                protected E callRetry() {
                    try {
                        return delegate.call();
                    } catch (final Throwable t) {
                        throw new RetryLaterRuntimeException(t);
                    }
                }
            }.call();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

}
