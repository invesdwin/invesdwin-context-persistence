package de.invesdwin.context.persistence.jpa.test;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.jpa.test.internal.PersistenceContext;

@Immutable
public enum PersistenceTestContext {
    MEMORY(PersistenceContext.TEST_MEMORY),
    SERVER(PersistenceContext.TEST_SERVER);

    private PersistenceContext persistenceContext;

    PersistenceTestContext(final PersistenceContext persistenceContext) {
        this.persistenceContext = persistenceContext;
    }

    public PersistenceContext getPersistenceContext() {
        return persistenceContext;
    }
}
