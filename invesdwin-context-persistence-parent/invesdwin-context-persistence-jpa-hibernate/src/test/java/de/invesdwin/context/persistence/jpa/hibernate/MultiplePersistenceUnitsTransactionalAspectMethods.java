package de.invesdwin.context.persistence.jpa.hibernate;

import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

import org.assertj.core.api.Fail;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import de.invesdwin.context.persistence.jpa.complex.TestDao;
import de.invesdwin.context.persistence.jpa.complex.TestEntity;
import de.invesdwin.util.assertions.Assertions;

@Configurable
@NotThreadSafe
public class MultiplePersistenceUnitsTransactionalAspectMethods {

    @Inject
    private TestDao testDao;
    @Inject
    private AnotherTestDao anotherTestDao;
    @Inject
    private AnotherTestDao anotherPuTestDao;

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