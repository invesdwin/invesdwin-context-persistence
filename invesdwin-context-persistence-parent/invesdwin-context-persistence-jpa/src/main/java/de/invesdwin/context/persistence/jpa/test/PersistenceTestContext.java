package de.invesdwin.context.persistence.jpa.test;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.jpa.test.internal.PersistenceContext;

@Immutable
public enum PersistenceTestContext {
    MEMORY(PersistenceContext.TEST_MEMORY),
    FILE(PersistenceContext.TEST_FILE),
    SERVER(PersistenceContext.TEST_SERVER);

    private final PersistenceContext persistenceContext;

    PersistenceTestContext(final PersistenceContext persistenceContext) {
        this.persistenceContext = persistenceContext;
    }

    public PersistenceContext getPersistenceContext() {
        return persistenceContext;
    }
}
