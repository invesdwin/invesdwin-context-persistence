package de.invesdwin.context.persistence.jpa.eclipselink;

import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import org.assertj.core.api.Fail;
import org.junit.Test;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import de.invesdwin.context.persistence.jpa.complex.TestDao;
import de.invesdwin.context.persistence.jpa.complex.TestEntity;
import de.invesdwin.context.persistence.jpa.test.APersistenceTest;
import de.invesdwin.util.assertions.Assertions;

// @ContextConfiguration(inheritLocations = false, locations = { APersistenceTest.CTX_TEST_SERVER })
@ThreadSafe
public class MultiplePersistenceUnitsTest extends APersistenceTest {

    @Inject
    private TestDao testDao;
    @Inject
    private AnotherTestDao anotherTestDao;
    @Inject
    private AnotherTestDao anotherPuTestDao;

    @Test
    public void testMultiplePersistenceUnitsWork() {
        new TransactionalAspectMethods().testMultiplePersistenceUnitsWork();
    }

    @Test
    public void testMultiplePersistenceUnitsWorkWithNestedTransactions() {
        new TransactionalAspectMethods().testMultiplePersistenceUnitsWorkWithNestedTransactions();
    }

    @Test
    public void testMultiplePersistenceUnitsWorkWithNestedRollback() {
        new TransactionalAspectMethods().testMultiplePersistenceUnitsWorkWithNestedRollback();
    }

    @Test
    public void testPropagationNever() throws InterruptedException {
        new TransactionalAspectMethods().testPropagationNever();
    }

    private class TransactionalAspectMethods {

        public void testMultiplePersistenceUnitsWork() {
            final TestEntity test = new TestEntity();
            test.setName("testMultiplePersistenceUnitsWork");
            testDao.save(test);

            final AnotherTestEntity weitererTest = new AnotherTestEntity();
            weitererTest.setName("testMultiplePersistenceUnitsWork");
            anotherTestDao.save(weitererTest);

            final AnotherPuTestEntity weitererTestPu = new AnotherPuTestEntity();
            weitererTestPu.setName("testMultiplePersistenceUnitsWork");
            anotherPuTestDao.save(weitererTest);
        }

        @Transactional
        public void testMultiplePersistenceUnitsWorkWithNestedTransactions() {
            testMultiplePersistenceUnitsWork();
        }

        public void testMultiplePersistenceUnitsWorkWithNestedRollback() {
            final long countBeforeTestDao = testDao.count();
            final long countBeforeAnotherTestDao = anotherTestDao.count();
            final long countBeforeAnotherPuTestDao = anotherPuTestDao.count();
            try {
                testMultiplePersistenceUnitsWorkWithRollback();
                Fail.fail("exception should have been thrown");
            } catch (final RuntimeException e) {
                Assertions.assertThat(e.getMessage()).contains("rollback reason");
            }
            Assertions.assertThat(testDao.count()).isEqualTo(countBeforeTestDao);
            Assertions.assertThat(anotherTestDao.count()).isEqualTo(countBeforeAnotherTestDao);
            Assertions.assertThat(anotherPuTestDao.count()).isEqualTo(countBeforeAnotherPuTestDao);
        }

        @Transactional
        private void testMultiplePersistenceUnitsWorkWithRollback() {
            testMultiplePersistenceUnitsWork();
            throw new RuntimeException("rollback reason");
        }

        @Transactional(propagation = Propagation.NEVER)
        public void testPropagationNever() throws InterruptedException {
            testDao.count();
            TimeUnit.SECONDS.sleep(1);
            testDao.count();
        }
    }

}
