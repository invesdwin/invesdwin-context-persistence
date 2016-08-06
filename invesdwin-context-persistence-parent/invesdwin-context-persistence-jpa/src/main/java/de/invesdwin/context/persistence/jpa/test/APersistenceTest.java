package de.invesdwin.context.persistence.jpa.test;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.test.ATest;

/**
 * Convenience base class to easily access helper methods.
 */
@NotThreadSafe
public abstract class APersistenceTest extends ATest implements IPersistenceTestHelper {

    @Override
    public void clearAllTables() {
        new PersistenceTestHelper().clearAllTables();
    }

}
