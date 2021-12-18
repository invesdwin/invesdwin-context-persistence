package de.invesdwin.context.persistence.jpa;

import javax.annotation.concurrent.ThreadSafe;

import org.junit.jupiter.api.Test;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import de.invesdwin.context.persistence.jpa.test.APersistenceTest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.assertions.Executable;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
@Transactional(value = PersistenceProperties.DEFAULT_TRANSACTION_MANAGER_NAME, propagation = Propagation.NEVER)
public class CachedTestDaoTest extends APersistenceTest {

    @Test
    public void testNoL2Cache() {
        Assertions.assertTimeout(new Duration(1, FTimeUnit.SECONDS), new Executable() {
            @Override
            public void execute() throws Throwable {
                new CachedTestDaoTransactionalAspectMethods().testNoL2Cache();
            }
        });
    }

    @Test
    public void testNoL2CacheWithQueryCache() {
        Assertions.assertTimeout(new Duration(1, FTimeUnit.SECONDS), new Executable() {
            @Override
            public void execute() throws Throwable {
                new CachedTestDaoTransactionalAspectMethods().testNoL2CacheWithQueryCache();
            }
        });
    }

    @Test
    public void testL2Cache() {
        Assertions.assertTimeout(new Duration(1, FTimeUnit.SECONDS), new Executable() {
            @Override
            public void execute() throws Throwable {
                new CachedTestDaoTransactionalAspectMethods().testL2Cache();
            }
        });
    }

    @Test
    public void testL2CacheWithoutQueryCache() {
        Assertions.assertTimeout(new Duration(1, FTimeUnit.SECONDS), new Executable() {
            @Override
            public void execute() throws Throwable {
                new CachedTestDaoTransactionalAspectMethods().testL2CacheWithoutQueryCache();
            }
        });
    }

    @Test
    public void testL2CacheWithQueryCache() {
        Assertions.assertTimeout(new Duration(1, FTimeUnit.SECONDS), new Executable() {
            @Override
            public void execute() throws Throwable {
                new CachedTestDaoTransactionalAspectMethods().testL2CacheWithQueryCache();
            }
        });
    }

}
