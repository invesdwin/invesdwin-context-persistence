package de.invesdwin.context.persistence.jpa.test;

import javax.annotation.concurrent.ThreadSafe;

import org.junit.Test;

import de.invesdwin.context.test.ATest;

@ThreadSafe
public abstract class APersistenceTestPreparer extends ATest {

    @Override
    public void setUpOnce() throws Exception {
        super.setUpOnce();
        new PersistenceTestHelper().clearAllTables();
    }

    @Test
    public abstract void prepare();

}
