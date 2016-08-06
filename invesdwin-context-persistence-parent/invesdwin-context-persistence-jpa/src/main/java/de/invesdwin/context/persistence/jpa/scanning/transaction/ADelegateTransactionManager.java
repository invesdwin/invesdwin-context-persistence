package de.invesdwin.context.persistence.jpa.scanning.transaction;

import javax.annotation.concurrent.ThreadSafe;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;

@ThreadSafe
public abstract class ADelegateTransactionManager implements PlatformTransactionManager {

    private PlatformTransactionManager delegate;

    private synchronized PlatformTransactionManager getDelegate() {
        if (delegate == null) {
            delegate = createDelegate();
        }
        return delegate;
    }

    protected abstract PlatformTransactionManager createDelegate();

    @Override
    public TransactionStatus getTransaction(final TransactionDefinition definition) {
        return getDelegate().getTransaction(definition);
    }

    @Override
    public void commit(final TransactionStatus status) {
        getDelegate().commit(status);
    }

    @Override
    public void rollback(final TransactionStatus status) {
        getDelegate().rollback(status);
    }

}
