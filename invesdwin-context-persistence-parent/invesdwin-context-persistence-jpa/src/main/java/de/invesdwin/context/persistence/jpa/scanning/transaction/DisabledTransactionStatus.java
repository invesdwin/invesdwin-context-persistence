package de.invesdwin.context.persistence.jpa.scanning.transaction;

import javax.annotation.concurrent.NotThreadSafe;

import org.springframework.transaction.TransactionStatus;

@NotThreadSafe
public class DisabledTransactionStatus implements TransactionStatus {

    @Override
    public Object createSavepoint() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollbackToSavepoint(final Object savepoint) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void releaseSavepoint(final Object savepoint) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNewTransaction() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasSavepoint() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRollbackOnly() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRollbackOnly() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCompleted() {
        throw new UnsupportedOperationException();
    }

}
