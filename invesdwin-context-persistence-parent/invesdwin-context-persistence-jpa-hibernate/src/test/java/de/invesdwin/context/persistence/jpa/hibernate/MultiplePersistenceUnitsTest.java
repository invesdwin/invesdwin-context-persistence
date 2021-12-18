package de.invesdwin.context.persistence.jpa.hibernate;

import javax.annotation.concurrent.ThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.persistence.jpa.test.APersistenceTest;

// @ContextConfiguration(inheritLocations = false, locations = { APersistenceTest.CTX_TEST_SERVER })
@ThreadSafe
public class MultiplePersistenceUnitsTest extends APersistenceTest {

    @Test
    public void testMultiplePersistenceUnitsWork() {
        new MultiplePersistenceUnitsTransactionalAspectMethods().testMultiplePersistenceUnitsWork();
    }

    @Test
    public void testMultiplePersistenceUnitsWorkWithNestedTransactions() {
        new MultiplePersistenceUnitsTransactionalAspectMethods()
                .testMultiplePersistenceUnitsWorkWithNestedTransactions();
    }

    @Test
    public void testMultiplePersistenceUnitsWorkWithNestedRollback() {
        new MultiplePersistenceUnitsTransactionalAspectMethods().testMultiplePersistenceUnitsWorkWithNestedRollback();
    }

    @Test
    public void testPropagationNever() throws InterruptedException {
        new MultiplePersistenceUnitsTransactionalAspectMethods().testPropagationNever();
    }

}
